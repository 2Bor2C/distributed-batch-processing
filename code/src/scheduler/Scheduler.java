package scheduler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import common.JobFactory;
import common.Opcode;

public class Scheduler {

  	
  int schedulerPort;
  Cluster cluster;
  int jobIdNext;
  JobCluster jobCluster;
  ExecutorService pool;
  CompletionService<String> return_pool;
  
  Scheduler(int p) {
    schedulerPort = p;
    cluster = new Cluster();
    jobIdNext = 1;
    jobCluster = new JobCluster();
    pool = Executors.newFixedThreadPool(8);  // since there are 8 workers
    return_pool = new ExecutorCompletionService<String>(pool);
 }

  public static void main(String[] args) {
    Scheduler scheduler = new Scheduler(Integer.parseInt(args[0]));
    scheduler.run();
  }

  public void run() {
    try{
    //	System.out.printf("Entering Run \n ");
      //create a ServerSocket listening at specified port
    	ServerSocket serverSocket = new ServerSocket(schedulerPort);
    	//accept connection from worker or client
        //Socket socket = serverSocket.accept();
		// ----------- Create thread here ??
		// Connector Thread -> while looped permanently waiting for connections
		Thread t1 = new Thread(new connector_thread(serverSocket));
		t1.start();
		//	System.out.printf("Thread 1 started \n ");
		Thread t2 = new Thread(new task_assigner());
		t2.start();
		//System.out.printf("Thread 2 started \n ");
		//insert join here
		t1.join();
		t2.join();
		
	   
    } catch(Exception e) {
      e.printStackTrace();
    }
      
    //serverSocket.close();
  }
  
	public class connector_thread implements Runnable
	{
		private ServerSocket serverSocket;
		
		public connector_thread(ServerSocket serverSocket)
		{
			this.serverSocket = serverSocket;
		}
		public void run()
		{
			try{
				while(true)
				{
					Socket socket = serverSocket.accept();
					Thread t = new Thread(new helper_thread(socket));
					t.start();
				}
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
	}	
	
	public class helper_thread implements Runnable
	{
		private Socket socket;
		
		public helper_thread(Socket socket)
		{
			this.socket = socket;
		}
		public void run()
		{
			int jobId = 0;
			try
			{
				//System.out.printf("Inside Helper Thread \n ");
				
				DataInputStream dis = new DataInputStream(socket.getInputStream());
				DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

				int code = dis.readInt();

				//a connection from worker reporting itself
				if(code == Opcode.new_worker)
				{
				  //include the worker into the cluster
					WorkerNode n = cluster.createWorkerNode( dis.readUTF(), dis.readInt());
					
					if( n == null)
					{
						dos.writeInt(Opcode.error);
					}
					else
					{
						dos.writeInt(Opcode.success);
						dos.writeInt(n.id);
					//	System.out.println("Worker "+n.id+" "+n.addr+" "+n.port+" created");
					}
					dos.flush();
					
				}
			try{
				//a connection from client submitting a job
				if(code == Opcode.new_job)
				{

					String className = dis.readUTF();
					long len = dis.readLong();
				  //send out the jobId
					jobId = jobIdNext++;
					dos.writeInt(jobId);
					dos.flush();
	
					//receive the job file and store it to the shared filesystem
					String fileName = new String("fs/."+jobId+".jar");
					FileOutputStream fos = new FileOutputStream(fileName);
					int count;
					byte[] buf = new byte[65536];
					while(len > 0) 
					{
						count = dis.read(buf);
						if(count > 0)
						{
							fos.write(buf, 0, count);
							len -= count;
						}
					}
					fos.flush();
					fos.close();
					int numOfTasks = JobFactory.getJob(fileName, className).getNumTasks();
					jobCluster.createJobsAllEntry(className, fileName, jobId, dis, dos, numOfTasks);
					dos.writeInt(Opcode.job_start);
					dos.flush();	
				
					Report report = new Report(0,0);
					while((jobCluster.isJobDone(jobId) == false) || (report != null))
					{
						report = jobCluster.receiveReport(jobId);
						if(report != null)
						{
							dos.writeInt(Opcode.job_print);
							dos.writeUTF("task "+ report.task+" finished on worker "+report.workerId );
				            dos.flush();
//				            System.out.printf("task: %d   ID:   %d \n", report.task, report.workerId);
						}	
						// next task to report
						
						
					}
					
				//	System.out.printf("Job Done ... Already >???");					
					dos.writeInt(Opcode.job_finish);
					dos.flush();
				}
				
				dis.close();
				dos.close();
				socket.close();
			}		 
			catch (SocketException e){
				System.out.println("Exception client dead \n "+e);
			//	if(jobId > 0)
				//jobCluster.removeJobsAllEntry(jobId);
			}		
			}		 
			catch (Exception e){
				System.out.println("Exception 1 \n "+e);
				e.printStackTrace();
				// Client cutoff ?
				
			}
		}	
		
		
    }
  	
	public class task_assigner implements Runnable
	{
		//private
		public task_assigner()
		{
		}
		public void run()
		{
			try
			{
				//int min_share = 0;
				//int tasks_assigned = 0;
				int jobTobeDone = 0;
				while(true)
				{
					//System.out.printf("Task Assigner Thread \n ");
					JobEntry entry;
					WorkerNode n;
					
				/*	if (jobCluster.returnJobsAllSize() != 0)
						min_share = cluster.returnFreeWorkersSize() / jobCluster.returnJobsAllSize();						
					
					if(min_share == 0)
						min_share = 1;
					*/
					
					for(int i=0; i < jobCluster.returnJobsAllSize(); i++)
					{					
						//Job DOne Already Check
						entry = jobCluster.getJobsAllEntry(i+1);
						if(jobCluster.isJobDone(entry.jobId))
						{
							jobTobeDone = i + 1;
							continue;
						}
						//{
							// Cancel the task 
							// possible options -> measure time per task for each job to pool and cancel one with the lowest ?
							// or cancel the last submitted job to pool ?
						//}
						n = cluster.getFreeWorkerNode();						
						int task_details[] = jobCluster.getzero(entry.jobId);
						//	System.out.printf("Start = %d", task_details[0]);
						if(task_details[0] == -1)
						{
							cluster.addFreeWorkerNode(n);
							continue;
						}
						// Tasks Assigned Per worker Calculation
						//tasks_assigned = (task_details[1] + 1) / min_share;
						/*if(tasks_assigned == 0)
							tasks_assigned = 1;
						*/
						if(jobCluster.isJobDone(entry.jobId))
						{
							jobTobeDone = i + 1;
							continue;
						}
							
						pool.submit(new task_worker(n, task_details[0], 1, entry.className, entry.jobId, entry.dos, task_details[1]));
						//jobCluster.updateTaskIdStart(tasks_assigned, entry.jobId);
						
						// Min Share Calculation
						//if (jobCluster.returnJobsAllSize() != 0)
						//{
							//min_share = cluster.returnFreeWorkersSize() / jobCluster.returnJobsAllSize();						
//							System.out.printf("No of Free Workers %d  no of Jobs %d \n ", cluster.returnFreeWorkersSize(), jobCluster.returnJobsAllSize());	
						//}
						//if(min_share == 0)
						//	min_share = 1;
						
						
						//System.out.printf("task details: start : %d  no : %d \n ", task_details[0], task_details[1]);
						jobCluster.setPrecommit(task_details[0], task_details[0], entry.jobId);
						
						
						//if All jobs not done and i reached last than reset i
						if(i == jobCluster.returnJobsAllSize() - 1)
							i = jobTobeDone - 1;
						
					}
					
					
				}
			}
			catch (Exception e) 
			{
				System.out.println("Exception 2 \n "+e);
				e.printStackTrace();
			}
		}
	}
	
	public class task_worker implements Callable<String>
	{
		//change object
		//private
		WorkerNode n;
		int taskIdStart;
		int tasksAssigned;
		String className;
		int jobId;
		DataOutputStream cos;
        int numOfTasks;
		public task_worker(WorkerNode n, int taskIdStart, int tasksAssigned, String className, int jobId, DataOutputStream cos, int numOfTasks)
		{
			this.n = n;
			this.taskIdStart = taskIdStart;
			this.tasksAssigned = tasksAssigned;
			this.className = className;
			this.jobId = jobId;
			this.cos = cos;
			this.numOfTasks = numOfTasks;
		}
		public String call()
		{
			try{
					Report report = new Report(0,0);
//					System.out.printf("Inside task Worker. No of Tasks this one is doing : % d \n ",tasksAssigned);
					Socket workerSocket = new Socket(n.addr, n.port);
					DataInputStream wis = new DataInputStream(workerSocket.getInputStream());
					DataOutputStream wos = new DataOutputStream(workerSocket.getOutputStream());
					wos.writeInt(Opcode.new_tasks);
					wos.writeInt(jobId);		// This needs to be input to thread !
					wos.writeUTF(className);	// This needs to be input to thread !
					wos.writeInt(taskIdStart); // This needs to be input to thread !
					wos.writeInt(tasksAssigned);  // This needs to be input to thread !
					wos.flush();
					while(wis.readInt() == Opcode.task_finish)
					{
						//cos.writeInt(Opcode.job_print);
						int i = wis.readInt();
						// System.out.println("task "+ i +" finished on worker "+n.id);
			            //cos.flush(); 
			            jobCluster.setPostcommit(i, i , jobId);
			            report.task = i;
			            report.workerId = n.id;
			            jobCluster.addReport(report, jobId);
			            jobCluster.printPostcommit(jobId);
					}
					wis.close();
					wos.close();
					workerSocket.close();
					//JobEntry entry = jobCluster.getJobsAllEntry(jobId);
					//Job done to be checked
					JobEntry entry = jobCluster.getJobsAllEntry(jobId);
					//System.out.printf("Job Entry Submitted \n ");
					
					int iterator = 0;
					for(iterator=0; iterator < entry.postcommit.size(); iterator++)
					{
						//System.out.printf("Value of Iterator: %d", iterator);
						if(entry.postcommit.get(iterator).equals(Integer.valueOf(0)))
							break;
						
					}
					//System.out.printf("Value of Iterator: %d", iterator);
					if(iterator == entry.postcommit.size())
					{
						jobCluster.setJobDone(jobId);
						//	System.out.printf("Job Id: %d done in this thread", jobId);
					}
					
					
					cluster.addFreeWorkerNode(n);
			}
			catch (EOFException e){
				cluster.removeWorkerNode(n);
				jobCluster.resetPrecommit(taskIdStart, tasksAssigned, jobId);
				jobCluster.printPrecommit(jobId);
				jobCluster.printPostcommit(jobId);
				System.out.println("EOF Exception  \n "+e);
				e.printStackTrace();
			}
			catch (ConnectException e)
			{
				cluster.removeWorkerNode(n);
				jobCluster.resetPrecommit(taskIdStart, tasksAssigned, jobId);
				jobCluster.printPrecommit(jobId);
				jobCluster.printPostcommit(jobId);
				System.out.println("Connect Exception \n "+e);
				e.printStackTrace();
			}
			catch (Exception e)
			{
				System.out.println("Exception ??? \n "+e);
			}
			finally {
				
			}
		return "Done";
		}
		
	}
  //the data structure for a cluster of worker nodes
  class Cluster {
    ArrayList<WorkerNode> workers; //all the workers
    LinkedList<WorkerNode> freeWorkers; //the free workers
    
    Cluster() {
      workers = new ArrayList<WorkerNode>();
      freeWorkers = new LinkedList<WorkerNode>();
    }

    WorkerNode createWorkerNode(String addr, int port) {
      WorkerNode n = null;

      synchronized(workers) {
        n = new WorkerNode(workers.size(), addr, port);
        workers.add(n);
      }
      addFreeWorkerNode(n);
      return n;
    }
        	
    
    WorkerNode getFreeWorkerNode() {
      WorkerNode n = null;

      try{
        synchronized(freeWorkers) {
          while(freeWorkers.size() == 0) {
            freeWorkers.wait();
          }
          n = freeWorkers.remove();
        }
        n.status = 2;
      } catch(Exception e) {
        e.printStackTrace();
      }

      return n;
    }

    void addFreeWorkerNode(WorkerNode n) {
      n.status = 1;
      synchronized(freeWorkers) {
        freeWorkers.add(n);
        freeWorkers.notifyAll();
      }
    }
	
    void removeWorkerNode(WorkerNode deadWorker)
    {
    	
    	// search for worker node: with worker id "id" and delete it
    	// this can be used when worker disconnected without being given a job
    	// the helper connector thread must catch this exception
    	synchronized(freeWorkers)
    	{
    		freeWorkers.remove(deadWorker);
    		
      	}
    	
    	
    }
    
	int returnFreeWorkersSize()
	{
			// is Synchronization required ??
			synchronized(freeWorkers){
				return freeWorkers.size();							
			}
	}
  }

  //the data structure of a worker node
  class WorkerNode {
    int id;
    String addr;
    int port;
    int status; //WorkerNode status: 0-sleep, 1-free, 2-busy, 4-failed

    WorkerNode(int i, String a, int p) {
      id = i;
      addr = a;
      port = p;
      status = 0;
    }
  }

  class ClientStream
	{
		Socket clientSocket;
		DataInputStream cis;
		DataOutputStream cos;
		int numTasksList;
			
		ClientStream(Socket clientSocket, DataInputStream cis, DataOutputStream cos, int numTasksList)
		{
			this.clientSocket = clientSocket;
			this.cis = cis;
			this.cos = cos;
			this.numTasksList = numTasksList ;   //required ?
		}
	}

class JobCluster 
	{
		ArrayList<JobEntry> jobsAll;
		//LinkedList<JobEntry> jobsRemaining;
	// Remove JObsremaining
		JobCluster() 
		{
			jobsAll = new ArrayList<JobEntry>();
			//jobsRemaining = new LinkedList<JobEntry>();
		}
		
		void printPostcommit(int jobId)
		{
			int index = jobId - 1;
			synchronized(jobsAll)
			{
				JobEntry entry = jobsAll.get(index);
				//System.out.printf(" Postcommit : \n ");
				for(int i = 0; i < entry.postcommit.size(); i++)
				{
				//	System.out.printf(" %d ", entry.postcommit.get(i).intValue());
				}
			}
		}
		void printPrecommit(int jobId)
		{
			int index = jobId - 1;
			synchronized(jobsAll)
			{
				JobEntry entry = jobsAll.get(index);
			//	System.out.printf(" Precommit : \n ");
				for(int i = 0; i < entry.precommit.size(); i++)
				{
				//	System.out.printf(" %d ", entry.precommit.get(i).intValue());
				}
			}
		}
		void addReport(Report report, int jobId)
		{
			int index = jobId - 1;
			synchronized(jobsAll)
			{
				JobEntry entry = jobsAll.get(index);
				entry.reported.add(report);
				jobsAll.set(index, entry);
			}
			
		}
		
		Report receiveReport(int jobId)
		{
			Report report = null;
			int index = jobId - 1;
			synchronized(jobsAll)
			{
				JobEntry entry = jobsAll.get(index);
				if(entry.reported.isEmpty() == false)
				{
					report = entry.reported.remove(0);
				}
			return report;
			}
		}
		int[] getzero (int jobId)
		{
			int index = jobId - 1;
		//	System.out.printf("In get zero Function");
			int iterator = 0;
			int iterator1 = 0;
			int start = 0;
			int end = 0;
			synchronized (jobsAll)
			{
				JobEntry entry = jobsAll.get(index);
				for (iterator = 0; iterator < entry.precommit.size(); iterator++)
				{
					if (entry.precommit.get(iterator).equals(Integer.valueOf(0)))
					{					
						start = iterator;
						break;
					}
				}
			//	System.out.printf("Value of start in getzero is %d", start);
				if(iterator == entry.precommit.size())
				{
					start = -1;
					end = -1;
				}
				else{
				for(iterator1 = start; iterator1 < entry.precommit.size(); iterator1++)
				{  
						if(entry.precommit.get(iterator1).equals(Integer.valueOf(1)))
						{
							end = iterator1;
							break;
						}
				}		
				if(iterator1 == entry.precommit.size())
				{
						end = iterator1;
				}
				}				
								
			}					
			
			return new int[] {start,(end - start - 1)};
		}
		void setPrecommit(int start, int end, int jobId)
		{
			int index = jobId - 1;
			synchronized (jobsAll){
				JobEntry entry = jobsAll.get(index);
				for(int i = start; i <= end; i++)
				{
					entry.precommit.set(i, 1);
					jobsAll.set(index, entry);
				}
				
			}
		}
		void setPostcommit(int start, int end, int jobId)
		{
			int index = jobId - 1;
			synchronized (jobsAll){
				JobEntry entry = jobsAll.get(index);
				for(int i = start; i <= end; i++)
				{
					entry.postcommit.set(i, 1);
					jobsAll.set(index, entry);
				}
			}
				
		}
		void resetPrecommit(int start, int tasks_assigned, int jobId)
		{
			int index = jobId - 1;
			synchronized (jobsAll){
				JobEntry entry = jobsAll.get(index);
				
				
				for(int i = start; i < start + tasks_assigned; i++)
				{
					entry.precommit.set(i, entry.postcommit.get(i));  
				}
					
				
			}
		}
		
		
		
		void updateTaskIdStart(int tasks_assigned, int jobId)
		{
			int index = jobId - 1;
			synchronized(jobsAll)
			{
				JobEntry entry = jobsAll.get(index);
				entry.taskIdStart += tasks_assigned;
				jobsAll.set(index, entry);
			}	
		}
		void setJobDone(int jobId)
		{
			int index = jobId - 1;
			synchronized(jobsAll)
			{
				JobEntry entry = jobsAll.get(index);
				entry.jobDone = 1;
				jobsAll.set(index, entry);
			}
		}
		
		boolean isJobDone(int jobId)
		{
			int index = jobId - 1;
			synchronized(jobsAll)
			{
				JobEntry entry = jobsAll.get(index);
				if(entry.jobDone == 1)
					return true;
				else
					return false;
				
			}
		}
		
		JobEntry createJobsAllEntry(String className, String fileName, int jobId, DataInputStream dis, DataOutputStream dos, int numOfTasks)
		{
			JobEntry entry = null;
			synchronized(jobsAll)
			{
				entry = new JobEntry(className, fileName, jobId, dis, dos, numOfTasks);
				jobsAll.add(entry);
				jobsAll.notifyAll();
			}
			//addJobsRemaining(entry);
			return entry;
		}
 
		JobEntry getJobsAllEntry(int jobId)
		{
			JobEntry entry = null;
			int index = jobId - 1;
			try 
			{
				synchronized(jobsAll)
				{
					
					if(jobsAll.size() == 0)
					{
						jobsAll.wait();
					}
					else
					{
						for(int i = 0; i < jobsAll.size(); i++)
						{
							 entry = jobsAll.get(i);
							 
							 if(entry.jobDone != 1)
							 {
								 break;
							 }
						}
					}
					if(entry == null)
					{
						jobsAll.wait();
						entry = jobsAll.get(index);
					}
				}
			//entry.status = 2;
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return entry;
		}
		
		JobEntry removeJobsAllEntry(int jobId)
		{
			int index = jobId - 1;
			JobEntry entry = null;
			try 
			{
				
				synchronized(jobsAll)
				{
										
					if(jobsAll.size() == 0)
					{
						return entry;  /// Wait ?
					}
					entry = jobsAll.remove(index);
					
				}
			//entry.status = 2;
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
			return entry;
		
		}
		
		int returnJobsAllSize()
		{
			try
			{
					synchronized(jobsAll)
					{
						if(jobsAll.size() == 0)
						{
							jobsAll.wait();
						}
													
					}
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
			
			return jobsAll.size();
		}
		
	}
		
		

		class JobEntry 
		{
			ArrayList<Integer> precommit; 
			ArrayList<Integer> postcommit;
			ArrayList<Report> reported;
			String className;
			String fileName;
			int jobId;
			int status;
			DataInputStream dis;
			DataOutputStream dos;
			int numOfTasks;
			int jobDone;
			int taskIdStart;
		
			JobEntry(String className, String fileName, int jobId, DataInputStream dis, DataOutputStream dos, int numOfTasks)
			{
				postcommit = new ArrayList<Integer>();
				precommit = new ArrayList<Integer>();
				reported = new ArrayList<Report>();
				this.className = className;
				this.fileName = fileName;
				for(int i=0; i < numOfTasks; i++)
				{
					postcommit.add(0);
					precommit.add(0);
				}
				this.jobId = jobId;
				this.status = 0;   //required ?
				this.dis = dis;
				this.dos = dos;					
				this.numOfTasks = numOfTasks;
				jobDone = 0;
				taskIdStart = 0;
				
			}
		}
		
		  class Report
			{
				int workerId;
				int task;
								
				Report(int workerId, int task)
				{
					this.workerId = workerId;
					this.task = task;
				}
			}	
}		
   
 /*
	public class Message
	{
		private String msg;
		public Message(String str)
		{
			this.msg = str;
		}	
		public String getMsg()
		{
			return msg;
		}
		public void setMsg(String str)
		{
			this.msg = str;
		}
	}
	*/
