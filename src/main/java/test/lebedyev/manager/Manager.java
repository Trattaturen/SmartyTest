package test.lebedyev.manager;

import java.rmi.Remote;
import java.rmi.RemoteException;

import test.lebedyev.worker.Worker;

public interface Manager extends Remote
{
    /**
     * A method that wakes manager from waiting.
     * This method is called by Worker
     * 
     * @param worker
     *            - worker that calls this method
     * @throws RemoteException
     */
    public void wakeUp(Worker worker) throws RemoteException;
}
