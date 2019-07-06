package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;

public class SortMergeOperator extends JoinOperator {
    public SortMergeOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        // for HW4
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new SortMergeIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */

        private String leftTableName;
        private String rightTableName;
        private RecordIterator leftIterator;
        private RecordIterator rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked = false;
        Comparator<Record> comparator = new LR_RecordComparator();

        public SortMergeIterator() throws QueryPlanException, DatabaseException {
            super();

            leftTableName = (new SortOperator(getTransaction(), getLeftTableName(), new LeftRecordComparator())).sort();
            rightTableName = (new SortOperator(getTransaction(), getRightTableName(), new RightRecordComparator())).sort();

            this.leftIterator = SortMergeOperator.this.getRecordIterator(leftTableName);
            this.rightIterator = SortMergeOperator.this.getRecordIterator(rightTableName);

            this.nextRecord = null;

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            try {
                fetchNextRecord();
            } catch (DatabaseException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Record result = nextRecord;
            try {
                fetchNextRecord();
            } catch (DatabaseException e) {
                nextRecord = null;
            }
            return result;
        }

        private void fetchNextRecord() throws DatabaseException {
            if (this.leftRecord == null) { throw new DatabaseException("No new record to fetch"); }
            nextRecord = null;
            int comparison;
            while (leftRecord != null && rightRecord != null && nextRecord == null) {
                comparison = comparator.compare(leftRecord, rightRecord);
                if (marked) {
                    // left record matched the previous right record
                    if (comparison == 0) { //match!
                        joinLeftRightAsNext();
                        rightRecord =  rightIterator.hasNext() ? rightIterator.next() : null;
                        if (rightRecord == null) {
                            leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                            rightIterator.reset();
                            rightRecord = rightIterator.next();
                        }
                    } else if (comparison < 0) {
                        // invariant: marked
                        rightIterator.reset();
                        rightRecord =  rightIterator.next();
                        leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                    } else {
                        // left record is bigger than right record
                        marked = false;
                    }
                } else {
                    if (comparison < 0) {
                        leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                    } else if (comparison > 0) {
                        rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    } else {
                        rightIterator.mark();
                        marked = true;
                    }
                }
            }
        }

        private void joinLeftRightAsNext() {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            nextRecord = new Record(leftValues);
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }

        /**
        * Left-Right Record comparator
        * o1 : leftRecord
        * o2: rightRecord
        */
        private class LR_RecordComparator implements Comparator<Record> {
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
