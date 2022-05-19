import java.util.ArrayList;
import java.util.List;

public class Consumer {
    private final Long lagCapacity;

    private double remainingArrivalCapacity;
    private List<Partition> assignedPartitions;
    private final double arrivalCapacity;
    private Long remainingLagCapacity;

    private final int id;

    public int getId() {
        return id;
    }


    public Consumer(int id, Long lagCapacity, double arrivalCapacity) {
        this.lagCapacity = lagCapacity;
        this.arrivalCapacity = arrivalCapacity;
        this.id=id;

        this.remainingLagCapacity = lagCapacity;
        this.remainingArrivalCapacity = arrivalCapacity;
        assignedPartitions = new ArrayList<>();
    }



    public Long getRemainingLagCapacity() {
        return remainingLagCapacity;
    }




    public double getRemainingArrivalCapacity() {
        return remainingArrivalCapacity;
    }




    //TODO attention to when bin packing using average arrival rates or average lag
    //TODO set remaining capacities accordingly

    public void  assignPartition(Partition partition) {
        assignedPartitions.add(partition);
        remainingLagCapacity -= partition.getLag();
        remainingArrivalCapacity -= partition.getArrivalRate();
    }

    @Override
    public String toString() {
        return "Consumer{" + "id="+ id +
                ",  lagCapacity=" +lagCapacity +
                ", remainingArrivalCapacity=" + String.format("%.2f",remainingArrivalCapacity) +
                ", assignedPartitions=" + assignedPartitions +
                ", arrivalCapacity=" + String.format("%.2f",arrivalCapacity) +
                ", remainingLagCapacity=" + remainingLagCapacity +
                '}';
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = lagCapacity != null ? lagCapacity.hashCode() : 0;
        temp = Double.doubleToLongBits(remainingArrivalCapacity);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (assignedPartitions != null ? assignedPartitions.hashCode() : 0);
        temp = Double.doubleToLongBits(arrivalCapacity);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (remainingLagCapacity != null ? remainingLagCapacity.hashCode() : 0);
        return result;
    }
    public void setRemainingArrivalCapacity(double remainingArrivalCapacity) {
        this.remainingArrivalCapacity = remainingArrivalCapacity;
    }
    public void setRemainingLagCapacity(Long remaininglagcapacity) {
        this.remainingLagCapacity = remaininglagcapacity;
    }
    public List<Partition> getAssignedPartitions() {
        return assignedPartitions;
    }

    public void setAssignedPartitions(List<Partition> assignedPartitions) {
        this.assignedPartitions = assignedPartitions;
    }

}
