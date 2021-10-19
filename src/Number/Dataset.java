package Number;

public enum Dataset {
    HEIGHTS(140.0, 200.0),
    AGES(20, 80)
    ;
    private final double low;
    private final double high;

    Dataset(double low, double high){
        this.low = low;
        this.high = high;
    }

    public double low() {
        return low;
    }

    public double high() {
        return high;
    }
}
