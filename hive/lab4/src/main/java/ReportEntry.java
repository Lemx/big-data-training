public class ReportEntry {

    private final String table;
    private final Long size;
    private final Integer queryNumber;
    private final Long executionTime;
    private final String comment;

    public String getTable() {
        return table;
    }

    public Long getSize() {
        return size;
    }

    public Integer getQueryNumber() {
        return queryNumber;
    }

    public Long getExecutionTime() {
        return executionTime;
    }

    public String getComment() {
        return comment;
    }

    public ReportEntry(String table, Long size, Integer queryNumber, Long executionTime, String comment) {

        this.table = table;
        this.size = size;
        this.queryNumber = queryNumber;
        this.executionTime = executionTime;
        this.comment = comment;
    }
}
