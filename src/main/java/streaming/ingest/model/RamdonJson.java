package streaming.ingest.model;

public class RamdonJson {

    private String name;
    private long key;
    private String desc;

    public RamdonJson() {
    }

    public RamdonJson(String name, long key, String desc) {
        this.name = name;
        this.key = key;
        this.desc = desc;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getKey() {
        return this.key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public String getDesc() {
        return this.desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }


    @Override
    public String toString() {
        return "{" +
            " name='" + getName() + "'" +
            ", key='" + getKey() + "'" +
            ", desc='" + getDesc() + "'" +
            "}";
    }

}


