package it.unitn.ds1;

import java.util.Objects;

public class Lock {
    //Lock depends on client name, coordinator key and the request number
    String clientName;
    int coordinatorId;
    int counterRequest;

    Lock(String clientName, int coordinatorId, int counterRequest){
        this.clientName = clientName;
        this.coordinatorId = coordinatorId;
        this.counterRequest = counterRequest;
    }

    public void setCounterRequest(int counterRequest) {
        this.counterRequest = counterRequest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Lock)) return false;
        Lock lock = (Lock) o;
        return coordinatorId == lock.coordinatorId && counterRequest == lock.counterRequest && Objects.equals(clientName, lock.clientName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientName, coordinatorId, counterRequest);
    }

    @Override
    public String toString() {
        return "Lock{" +
                "clientName='" + clientName + '\'' +
                ", coordinatorId=" + coordinatorId +
                ", counterRequest=" + counterRequest +
                '}';
    }
}
