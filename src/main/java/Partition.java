public class Partition implements Comparable<Partition> {

    private int id;
    private long lag;
    private double arrivalRate;

    private double previousArrivalRate;
    //TODO externlize windown length and add wondows for
    //TODO rate of arrival rate d/dt(arrival rate)
    // TODO and window for the lag rate d/dt (lag)
    private double[] arrivalRateWindow = new double[4];
    private Long[] lagWindow = new Long[4];
    private double[] rateForarrivalRateWindow = new double[4];
    private double[] rateForLagWindow = new double[4];


    public Partition(int id, long lag, double arrivalRate) {
        this.id = id;
        this.lag = lag;
        this.arrivalRate = arrivalRate;


        for (int i = 0; i < 4; i++) {
            //offsetWindow[i] = 0L;
            arrivalRateWindow[i] = 0.0;
            lagWindow[i] = 0L;
            rateForarrivalRateWindow[i] = 0.0;
            rateForLagWindow[i] = 0.0;
        }
    }

    //TODO is that really needed?
    public double getPreviousArrivalRate() {
        return previousArrivalRate;
    }
    public void setPreviousArrivalRate(double previousArrivalRate) {
        this.previousArrivalRate = previousArrivalRate;
    }
    public double[] getArrivalRateWindow() {
        return arrivalRateWindow;
    }
    public void setArrivalRateWindow(double[] arrivalRateWindow) {
        this.arrivalRateWindow = arrivalRateWindow;
    }

    public double getAverageArrivalRate() {
        double averageArrivalRate = 0.0;
        for (int i = 0; i < 4; i++) {
            //offsetWindow[i] = 0L;
            averageArrivalRate += arrivalRateWindow[i];
        }
        return averageArrivalRate / 4.0;
    }

    public double getAverageLag() {
        Long averageLag = 0L;
        for (int i = 0; i < 4; i++) {
            averageLag += lagWindow[i];
        }
        return (double) averageLag / 4.0;
    }

    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public long getLag() {
        return lag;
    }

    public void setLag(long lag) {
        this.lag = lag;

        for (int i = 2; i >= 0; i--) {
            lagWindow[i + 1] = lagWindow[i];
            rateForLagWindow[i + 1] = rateForLagWindow[i];
        }
        lagWindow[0] = lag;
        rateForLagWindow[0] = (double) (lagWindow[0] - lagWindow[1]) / Controller.doublesleep;

    }

    public double getArrivalRate() {
        return arrivalRate;
    }

    public void setArrivalRate(double arrivalRate) {
        this.arrivalRate = arrivalRate;

        for (int i = 2; i >= 0; i--) {
            arrivalRateWindow[i + 1] = arrivalRateWindow[i];
            rateForarrivalRateWindow[i + 1] = rateForarrivalRateWindow[i];
        }
        arrivalRateWindow[0] = arrivalRate;
        rateForarrivalRateWindow[0] = (double) (arrivalRateWindow[0] - arrivalRateWindow[1]) / Controller.doublesleep;
    }




    private double getAverageRateForLag() {
        double averageRateForLag = 0.0;
        for (int i = 0; i < 4; i++) {
            //offsetWindow[i] = 0L;
            averageRateForLag += rateForLagWindow[i];
        }
        return averageRateForLag / 4.0;

    }

    private double getAverageRateForArrivalRate() {
        double averageRateForArrivalRate = 0.0;
        for (int i = 0; i < 4; i++) {
            //offsetWindow[i] = 0L;
            averageRateForArrivalRate += rateForarrivalRateWindow[i];
        }
        return averageRateForArrivalRate / 4.0;
    }


    @Override
    public String toString() {
        return "Partition{" +
                "id= " + id +
                ", lag= " + lag +
                ", arrivalRate= " + String.format("%.2f", arrivalRate) +
                ", averageArrivalRate= " + String.format("%.2f", getAverageArrivalRate()) +
                ", averageLag= " + String.format("%.2f", getAverageLag()) +
                "}\n";
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Partition partition = (Partition) o;

        if (id != partition.id) return false;
        if (lag != partition.lag) return false;
        return Double.compare(partition.arrivalRate, arrivalRate) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id;
        result = 31 * result + (int) (lag ^ (lag >>> 32));
        temp = Double.doubleToLongBits(arrivalRate);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public int compareTo(Partition o) {
        return Long.compare(lag, o.lag);
    }

    //TODO add corresponding windows for  lag rate (d/dt lag(t)), and a function to return its average etc...
    //TODO add corresponding window for rate of arrival rate, and a function to return the average rate of arrival rate
    //TODO customize and externalize parameters such as window size and wherever applicable
}
