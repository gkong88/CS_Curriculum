package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.Page;

import java.util.*;

public class SortOperator {
    private Database.Transaction transaction; //track operations you perform on data
    private String tableName; //reference which serves as key to access table in transaction
    private Comparator<Record> comparator; //record comparator
    private Schema operatorSchema; //schema for your records
    private int numBuffers; //how many buffs u got?
    private String sortedTableName = null; // output table name, persisted from constructor

    public SortOperator(Database.Transaction transaction, String tableName,
                        Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getNumMemoryPages();
    }

    public Schema computeSchema() throws QueryPlanException {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * Run is a subclass of the SortOperator (helper class)
     *
     *
     */
    public class Run {
        String tempTableName;

        public Run() throws DatabaseException {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                                     SortOperator.this.operatorSchema);
        }

        public void addRecord(List<DataBox> values) throws DatabaseException {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        public void addRecords(List<Record> records) throws DatabaseException {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        public Iterator<Record> iterator() throws DatabaseException {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        public String tableName() {
            return this.tempTableName;
        }
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) throws DatabaseException {
        ArrayList<Record> recordArrayList = new ArrayList<>();
        Iterator<Record> originalRunIterator = run.iterator();
        while (originalRunIterator.hasNext()) {
            recordArrayList.add(originalRunIterator.next());
        }
        recordArrayList.sort(comparator);
        Run sortedRun = new Run();
        sortedRun.addRecords(recordArrayList);
        return sortedRun;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
        Run sortedRun = new Run();
        ArrayList<Iterator<Record>> runIterators = new ArrayList<>();
        PriorityQueue<Pair<Record, Integer>> runMinsQueue = new PriorityQueue<>(
                (p1, p2) -> comparator.compare(p1.getFirst(), p2.getFirst()));
        int i = 0;
        for (Run run: runs) {
            Iterator<Record> runIterator = run.iterator();
            if (runIterator.hasNext()) {
                runIterators.add(runIterator);
                runMinsQueue.add(new Pair<>(runIterator.next(), i));
                i++;
            }
        }
        while (runMinsQueue.size() > 0) {
            Pair<Record, Integer> nextRecordPair = runMinsQueue.poll();
            sortedRun.addRecord(nextRecordPair.getFirst().getValues());
            int nextRecordIndex = nextRecordPair.getSecond();
            if (runIterators.get(nextRecordIndex).hasNext()) {
                runMinsQueue.add(new Pair<>(runIterators.get(nextRecordIndex).next(), nextRecordIndex));
            }
        }
        return sortedRun;
    }

    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time.
     */
    public List<Run> mergePass(List<Run> runs) throws DatabaseException {
        ArrayList<Run> mergedRuns = new ArrayList<>();
        int windowStart;
        int windowEnd = 0;
        while (windowEnd < runs.size()) {
            windowStart = windowEnd;
            windowEnd = Math.min(runs.size(), windowEnd + numBuffers - 1);
            mergedRuns.add(mergeSortedRuns(runs.subList(windowStart, windowEnd)));
        }
        return mergedRuns;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() throws DatabaseException {
        Iterator<Page> recordIterator = transaction.getPageIterator(tableName);
        recordIterator.next();
        List<Run> runs = new ArrayList<>();
        while (recordIterator.hasNext()) {
            BacktrackingIterator<Record> nextBlock = transaction.getBlockIterator(tableName, recordIterator, numBuffers-1);
            ArrayList<Record> blockRecords = new ArrayList<>();
            while (nextBlock.hasNext()) {
                blockRecords.add(nextBlock.next());
            }
            Run nextRun = new Run();
            nextRun.addRecords(blockRecords);
            runs.add(sortRun(nextRun));
        }
        while (runs.size() > 1) {
            runs = mergePass(runs);
        }
        return runs.get(0).tableName();
    }

    public Iterator<Record> iterator() throws DatabaseException {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());

        }
    }
    public Run createRun() throws DatabaseException {
        return new Run();
    }
}

