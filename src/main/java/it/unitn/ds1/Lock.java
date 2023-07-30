package it.unitn.ds1;

import java.util.Objects;

public class Lock {
    String clientName;
    int coordinatorId;

    Lock(String clientName, int coordinatorId){
        this.clientName = clientName;
        this.coordinatorId = coordinatorId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Lock)) return false;
        Lock lock = (Lock) o;
        return coordinatorId == lock.coordinatorId && Objects.equals(clientName, lock.clientName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientName, coordinatorId);
    }

    @Override
    public String toString() {
        return "Lock{" +
                "clientName='" + clientName + '\'' +
                ", coordinatorId=" + coordinatorId +
                '}';
    }
}
