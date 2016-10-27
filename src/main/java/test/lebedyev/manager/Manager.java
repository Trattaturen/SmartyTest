package test.lebedyev.manager;

import java.rmi.Remote;
import java.rmi.RemoteException;

import test.lebedyev.worker.Worker;

public interface Manager extends Remote
{
    public void wakeUp(Worker worker) throws RemoteException;
}
