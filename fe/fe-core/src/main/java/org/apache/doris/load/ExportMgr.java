// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.load;

import org.apache.doris.analysis.CancelExportStmt;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.exception.JobException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ExportMgr {
    private static final Logger LOG = LogManager.getLogger(ExportJob.class);
    private Map<Long, ExportJob> exportIdToJob = Maps.newHashMap(); // exportJobId to exportJob
    // dbid -> <label -> job>
    private Map<Long, Map<String, Long>> dbTolabelToExportJobId = Maps.newHashMap();

    // lock for protecting export jobs.
    // need to be added when creating or cancelling export job.
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public ExportMgr() {
    }

    public List<ExportJob> getJobs() {
        return Lists.newArrayList(exportIdToJob.values());
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void addExportJobAndRegisterTask(ExportJob job) throws Exception {
        writeLock();
        try {
            if (dbTolabelToExportJobId.containsKey(job.getDbId())
                    && dbTolabelToExportJobId.get(job.getDbId()).containsKey(job.getLabel())) {
                Long oldJobId = dbTolabelToExportJobId.get(job.getDbId()).get(job.getLabel());
                ExportJob oldJob = exportIdToJob.get(oldJobId);
                if (oldJob != null && oldJob.getState() != ExportJobState.CANCELLED) {
                    throw new LabelAlreadyUsedException(job.getLabel());
                }
            }
            unprotectAddJob(job);
            Env.getCurrentEnv().getEditLog().logExportCreate(job);
        } finally {
            writeUnlock();
        }
        // delete existing files
        if (Config.enable_delete_existing_files && Boolean.parseBoolean(job.getDeleteExistingFiles())) {
            if (job.getBrokerDesc() == null) {
                throw new AnalysisException("Local file system does not support delete existing files");
            }
            String fullPath = job.getExportPath();
            BrokerUtil.deleteDirectoryWithFileSystem(fullPath.substring(0, fullPath.lastIndexOf('/') + 1),
                    job.getBrokerDesc());
        }
        // ATTN: Must add task after edit log, otherwise the job may finish before adding job.
        try {
            for (int i = 0; i < job.getCopiedTaskExecutors().size(); i++) {
                Env.getCurrentEnv().getTransientTaskManager().addMemoryTask(job.getCopiedTaskExecutors().get(i));
            }
        } catch (Exception e) {
            // If there happens exceptions in `addMemoryTask`
            // we must update the state of export job to `CANCELLED`
            // because we have added this export in `ExportMgr`
            job.updateExportJobState(ExportJobState.CANCELLED, 0L, null,
                    ExportFailMsg.CancelType.RUN_FAIL, e.getMessage());
        }
        LOG.info("add export job. {}", job);
    }

    public void cancelExportJob(CancelExportStmt stmt) throws DdlException, AnalysisException {
        // List of export jobs waiting to be cancelled
        List<ExportJob> matchExportJobs = getWaitingCancelJobs(stmt);
        if (matchExportJobs.isEmpty()) {
            throw new DdlException("Export job(s) do not exist");
        }
        matchExportJobs = matchExportJobs.stream()
                .filter(job -> !job.isFinalState()).collect(Collectors.toList());
        if (matchExportJobs.isEmpty()) {
            throw new DdlException("All export job(s) are at final state (CANCELLED/FINISHED)");
        }

        // check auth
        checkCancelExportJobAuth(InternalCatalog.INTERNAL_CATALOG_NAME, stmt.getDbName(), matchExportJobs);
        // Must add lock to protect export job.
        // Because job may be cancelled when generating task executors,
        // the cancel process may clear the task executor list at same time,
        // which will cause ConcurrentModificationException
        writeLock();
        try {
            for (ExportJob exportJob : matchExportJobs) {
                // exportJob.cancel(ExportFailMsg.CancelType.USER_CANCEL, "user cancel");
                exportJob.updateExportJobState(ExportJobState.CANCELLED, 0L, null,
                        ExportFailMsg.CancelType.USER_CANCEL, "user cancel");
            }
        } catch (JobException e) {
            throw new AnalysisException(e.getMessage());
        } finally {
            writeUnlock();
        }
    }

    private List<ExportJob> getWaitingCancelJobs(
            String label, String state,
            Expression operator)
            throws AnalysisException {
        Predicate<ExportJob> jobFilter = buildCancelJobFilter(label, state, operator);
        readLock();
        try {
            return getJobs().stream().filter(jobFilter).collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    @VisibleForTesting
    public static Predicate<ExportJob> buildCancelJobFilter(
            String label, String state,
            Expression operator)
            throws AnalysisException {
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(label,
                CaseSensibility.LABEL.getCaseSensibility());

        return job -> {
            boolean labelFilter = true;
            boolean stateFilter = true;
            if (StringUtils.isNotEmpty(label)) {
                labelFilter = label.contains("%") ? matcher.match(job.getLabel()) :
                    job.getLabel().equalsIgnoreCase(label);
            }
            if (StringUtils.isNotEmpty(state)) {
                stateFilter = job.getState().name().equalsIgnoreCase(state);
            }

            if (operator != null && operator instanceof Or) {
                return labelFilter || stateFilter;
            }

            return labelFilter && stateFilter;
        };
    }

    /**
     * used for Nereids planner
     */
    public void cancelExportJob(
            String label,
            String state,
            Expression operator, String dbName)
            throws DdlException, AnalysisException {
        // List of export jobs waiting to be cancelled
        List<ExportJob> matchExportJobs = getWaitingCancelJobs(label, state, operator);
        if (matchExportJobs.isEmpty()) {
            throw new DdlException("Export job(s) do not exist");
        }
        matchExportJobs = matchExportJobs.stream()
            .filter(job -> !job.isFinalState()).collect(Collectors.toList());
        if (matchExportJobs.isEmpty()) {
            throw new DdlException("All export job(s) are at final state (CANCELLED/FINISHED)");
        }

        // check auth
        checkCancelExportJobAuth(InternalCatalog.INTERNAL_CATALOG_NAME, dbName, matchExportJobs);
        // Must add lock to protect export job.
        // Because job may be cancelled when generating task executors,
        // the cancel process may clear the task executor list at same time,
        // which will cause ConcurrentModificationException
        writeLock();
        try {
            for (ExportJob exportJob : matchExportJobs) {
                // exportJob.cancel(ExportFailMsg.CancelType.USER_CANCEL, "user cancel");
                exportJob.updateExportJobState(ExportJobState.CANCELLED, 0L, null,
                        ExportFailMsg.CancelType.USER_CANCEL, "user cancel");
            }
        } catch (JobException e) {
            throw new AnalysisException(e.getMessage());
        } finally {
            writeUnlock();
        }
    }

    public void checkCancelExportJobAuth(String ctlName, String dbName, List<ExportJob> jobs) throws AnalysisException {
        if (jobs.size() > 1) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), ctlName, dbName,
                            PrivPredicate.SELECT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED_ERROR,
                        PrivPredicate.SELECT.getPrivs().toString(), dbName);
            }
        } else {
            TableName tableName = jobs.get(0).getTableName();
            if (tableName == null) {
                return;
            }
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), ctlName, dbName,
                            tableName.getTbl(),
                            PrivPredicate.SELECT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                        PrivPredicate.SELECT.getPrivs().toString(), tableName.getTbl());
            }
        }
    }

    public void unprotectAddJob(ExportJob job) {
        exportIdToJob.put(job.getId(), job);
        dbTolabelToExportJobId.computeIfAbsent(job.getDbId(),
                k -> Maps.newHashMap()).put(job.getLabel(), job.getId());
    }

    private List<ExportJob> getWaitingCancelJobs(CancelExportStmt stmt) throws AnalysisException {
        Predicate<ExportJob> jobFilter = buildCancelJobFilter(stmt);
        readLock();
        try {
            return getJobs().stream().filter(jobFilter).collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    @VisibleForTesting
    public static Predicate<ExportJob> buildCancelJobFilter(CancelExportStmt stmt) throws AnalysisException {
        String label = stmt.getLabel();
        String state = stmt.getState();
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(label,
                CaseSensibility.LABEL.getCaseSensibility());

        return job -> {
            boolean labelFilter = true;
            boolean stateFilter = true;
            if (StringUtils.isNotEmpty(label)) {
                labelFilter = label.contains("%") ? matcher.match(job.getLabel()) :
                        job.getLabel().equalsIgnoreCase(label);
            }
            if (StringUtils.isNotEmpty(state)) {
                stateFilter = job.getState().name().equalsIgnoreCase(state);
            }

            if (stmt.getOperator() != null && CompoundPredicate.Operator.OR.equals(stmt.getOperator())) {
                return labelFilter || stateFilter;
            }

            return labelFilter && stateFilter;
        };
    }

    public ExportJob getJob(long jobId) {
        ExportJob job;
        readLock();
        try {
            job = exportIdToJob.get(jobId);
        } finally {
            readUnlock();
        }
        return job;
    }

    // used for `show export` statement
    // NOTE: jobid and states may both specified, or only one of them, or neither
    public List<List<String>> getExportJobInfosByIdOrState(
            long dbId, long jobId, String label, boolean isLabelUseLike, Set<ExportJobState> states,
            ArrayList<OrderByPair> orderByPairs, long limit) throws AnalysisException {

        long resultNum = limit == -1L ? Integer.MAX_VALUE : limit;
        LinkedList<List<Comparable>> exportJobInfos = new LinkedList<List<Comparable>>();
        PatternMatcher matcher = null;
        if (isLabelUseLike) {
            matcher = PatternMatcherWrapper.createMysqlPattern(label, CaseSensibility.LABEL.getCaseSensibility());
        }

        readLock();
        try {
            int counter = 0;
            for (ExportJob job : exportIdToJob.values()) {
                long id = job.getId();
                ExportJobState state = job.getState();
                String jobLabel = job.getLabel();

                if (job.getDbId() != dbId) {
                    continue;
                }

                if (jobId != 0 && id != jobId) {
                    continue;
                }

                if (!Strings.isNullOrEmpty(label)) {
                    if (!isLabelUseLike && !jobLabel.equals(label)) {
                        // use = but does not match
                        continue;
                    } else if (isLabelUseLike && !matcher.match(jobLabel)) {
                        // use like but does not match
                        continue;
                    }
                }

                if (states != null) {
                    if (!states.contains(state)) {
                        continue;
                    }
                }

                // check auth
                if (isJobShowable(job)) {
                    exportJobInfos.add(composeExportJobInfo(job));
                }

                if (++counter >= resultNum && orderByPairs == null) {
                    break;
                }
            }
        } finally {
            readUnlock();
        }

        // order by
        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<List<Comparable>>(0);
        }
        Collections.sort(exportJobInfos, comparator);

        List<List<String>> results = Lists.newArrayList();
        int counter = 0;
        for (List<Comparable> list : exportJobInfos) {
            results.add(list.stream().map(e -> e.toString()).collect(Collectors.toList()));
            if (++counter >= resultNum) {
                break;
            }
        }

        return results;
    }

    public List<List<String>> getExportJobInfos(long limit) {
        long resultNum = limit == -1L ? Integer.MAX_VALUE : limit;
        LinkedList<List<Comparable>> exportJobInfos = new LinkedList<List<Comparable>>();

        readLock();
        try {
            int counter = 0;
            for (ExportJob job : exportIdToJob.values()) {
                // check auth
                if (isJobShowable(job)) {
                    exportJobInfos.add(composeExportJobInfo(job));
                }

                if (++counter >= resultNum) {
                    break;
                }
            }
        } finally {
            readUnlock();
        }

        // order by
        ListComparator<List<Comparable>> comparator = null;
        // sort by id asc
        comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(exportJobInfos, comparator);

        List<List<String>> results = Lists.newArrayList();
        for (List<Comparable> list : exportJobInfos) {
            results.add(list.stream().map(e -> e.toString()).collect(Collectors.toList()));
        }

        return results;
    }

    public boolean isJobShowable(ExportJob job) {
        TableName tableName = job.getTableName();
        if (tableName == null || tableName.getTbl().equals("DUMMY")) {
            // forward compatibility, no table name is saved before
            Database db = Env.getCurrentInternalCatalog().getDbNullable(job.getDbId());
            if (db == null) {
                return false;
            }
            if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                    InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(), PrivPredicate.SHOW)) {
                return false;
            }
        } else {
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), tableName.getCtl(),
                    tableName.getDb(), tableName.getTbl(),
                    PrivPredicate.SHOW)) {
                return false;
            }
        }

        return true;
    }

    private List<Comparable> composeExportJobInfo(ExportJob job) {
        List<Comparable> jobInfo = new ArrayList<Comparable>();

        jobInfo.add(job.getId());
        jobInfo.add(job.getLabel());
        jobInfo.add(job.getState().name());
        jobInfo.add(job.getProgress() + "%");

        // task infos
        Map<String, Object> infoMap = Maps.newHashMap();
        List<String> partitions = job.getPartitionNames();
        if (partitions == null) {
            partitions = Lists.newArrayList();
            partitions.add("*");
        }
        infoMap.put("db", job.getTableName().getDb());
        infoMap.put("tbl", job.getTableName().getTbl());
        if (!StringUtils.isEmpty(job.getWhereStr())) {
            infoMap.put("where expr", job.getWhereStr());
        }
        infoMap.put("partitions", partitions);
        infoMap.put("broker", job.getBrokerDesc().getName());
        infoMap.put("column_separator", job.getColumnSeparator());
        infoMap.put("format", job.getFormat());
        infoMap.put("with_bom", job.getWithBom());
        infoMap.put("line_delimiter", job.getLineDelimiter());
        infoMap.put("columns", job.getColumns());
        infoMap.put("tablet_num", job.getTabletsNum());
        infoMap.put("max_file_size", job.getMaxFileSize());
        infoMap.put("delete_existing_files", job.getDeleteExistingFiles());
        infoMap.put("parallelism", job.getParallelism());
        infoMap.put("data_consistency", job.getDataConsistency());
        jobInfo.add(new Gson().toJson(infoMap));
        // path
        jobInfo.add(job.getExportPath());

        jobInfo.add(TimeUtils.longToTimeString(job.getCreateTimeMs()));
        jobInfo.add(TimeUtils.longToTimeString(job.getStartTimeMs()));
        jobInfo.add(TimeUtils.longToTimeString(job.getFinishTimeMs()));
        jobInfo.add(job.getTimeoutSecond());

        // error msg
        if (job.getState() == ExportJobState.CANCELLED) {
            ExportFailMsg failMsg = job.getFailMsg();
            jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
        } else {
            jobInfo.add(FeConstants.null_string);
        }

        // outfileInfo
        if (job.getState() == ExportJobState.FINISHED) {
            jobInfo.add(job.getOutfileInfo());
        } else {
            jobInfo.add(FeConstants.null_string);
        }

        return jobInfo;
    }

    public void removeOldExportJobs() {
        long currentTimeMs = System.currentTimeMillis();

        writeLock();
        try {
            Iterator<Map.Entry<Long, ExportJob>> iter = exportIdToJob.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, ExportJob> entry = iter.next();
                ExportJob job = entry.getValue();
                if ((currentTimeMs - job.getCreateTimeMs()) / 1000 > Config.history_job_keep_max_second
                        && (job.getState() == ExportJobState.CANCELLED
                        || job.getState() == ExportJobState.FINISHED)) {
                    iter.remove();
                    Map<String, Long> labelJobs = dbTolabelToExportJobId.get(job.getDbId());
                    if (labelJobs != null) {
                        labelJobs.remove(job.getLabel());
                        if (labelJobs.isEmpty()) {
                            dbTolabelToExportJobId.remove(job.getDbId());
                        }
                    }
                }
            }

            if (exportIdToJob.size() > Config.max_export_history_job_num) {
                List<Map.Entry<Long, ExportJob>> jobList = new ArrayList<>(exportIdToJob.entrySet());
                jobList.sort(Comparator.comparingLong(entry -> entry.getValue().getCreateTimeMs()));
                while (exportIdToJob.size() > Config.max_export_history_job_num) {
                    // Remove the oldest job
                    Map.Entry<Long, ExportJob> oldestEntry = jobList.remove(0);
                    exportIdToJob.remove(oldestEntry.getKey());
                    Map<String, Long> labelJobs = dbTolabelToExportJobId.get(oldestEntry.getValue().getDbId());
                    if (labelJobs != null) {
                        labelJobs.remove(oldestEntry.getValue().getLabel());
                        if (labelJobs.isEmpty()) {
                            dbTolabelToExportJobId.remove(oldestEntry.getValue().getDbId());
                        }
                    }
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayCreateExportJob(ExportJob job) {
        writeLock();
        try {
            unprotectAddJob(job);
        } finally {
            writeUnlock();
        }
    }

    public void replayUpdateJobState(ExportJobStateTransfer stateTransfer) {
        writeLock();
        try {
            LOG.info("replay update export job: {}, {}", stateTransfer.getJobId(), stateTransfer.getState());
            ExportJob job = exportIdToJob.get(stateTransfer.getJobId());
            job.replayExportJobState(stateTransfer.getState());
            job.setStartTimeMs(stateTransfer.getStartTimeMs());
            job.setFinishTimeMs(stateTransfer.getFinishTimeMs());
            job.setFailMsg(stateTransfer.getFailMsg());
            job.setOutfileInfo(stateTransfer.getOutFileInfo());
        } finally {
            writeUnlock();
        }
    }

    public long getJobNum(ExportJobState state, long dbId) {
        int size = 0;
        readLock();
        try {
            for (ExportJob job : exportIdToJob.values()) {
                if (job.getState() == state && job.getDbId() == dbId) {
                    ++size;
                }
            }
        } finally {
            readUnlock();
        }
        return size;
    }

    public long getJobNum(ExportJobState state) {
        int size = 0;
        readLock();
        try {
            for (ExportJob job : exportIdToJob.values()) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME,
                                Env.getCurrentEnv().getCatalogMgr().getDbNullable(job.getDbId()).getFullName(),
                                PrivPredicate.LOAD)) {
                    continue;
                }

                if (job.getState() == state) {
                    ++size;
                }
            }
        } finally {
            readUnlock();
        }
        return size;
    }
}
