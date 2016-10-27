package test.lebedyev.worker;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Worker extends Remote
{
    /**
     * Method that makes worker start a new WorkerThread
     * 
     * @param json
     *            a record from redis that should be processed
     * @throws RemoteException
     */
    public void execute(String json) throws RemoteException;

    /**
     * @return true if worker finished, false - otherwise
     * @throws RemoteException
     */
    public boolean isFinished() throws RemoteException;

    /**
     * @return name of a current worker
     * @throws RemoteException
     */
    public String getName() throws RemoteException;
}
