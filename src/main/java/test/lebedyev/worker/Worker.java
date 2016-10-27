package test.lebedyev.worker;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Worker extends Remote
{
    public void execute(String json) throws RemoteException;

    public boolean isFinished() throws RemoteException;

    public String getName() throws RemoteException;
}
