package hu.mta.sztaki.lpds.cloud.simulator.wms;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;


import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.VMKeeper;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
//import scheduling.algorithms.MaxMin;
//import scheduling.algorithms.MinMin;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.mta.sztaki.lpds.cloud.simulator.wms.WorkflowJob.StateJob;



public class Engine  implements WorkflowJob.StateChange {
	
	/**
	 * the iaas services to be used for executing the jobs
	 */
	protected IaaSService target;
	/**
	 * the list of repositories that belong to the target iaas services.
	 */
	protected Repository repo;
	
	/**
	 * the virtual appliance that will be used as the generic image for each VM in
	 * the clouds
	 */
	protected VirtualAppliance va;
	/**
	 * the list of Virtual Machine keepers - the list of VMs that might be alive
	 */
	protected Set<VMKeeper> pooledVMs = new TreeSet<VMKeeper>(VMKeeper.compareKeepers);
	
	protected HashMap<String, WorkflowJob> jobs;
	
	/**
	 * maximum number of cores in the biggest physical machine
	 */
	protected long maxmachinecores = 0;
	/**
	 * maximum number of physical machines
	 */
	protected long maxIaaSmachines = 0;
	/**
	 * the default processing power share to be requested during the resource
	 * allocation for the VMs - allows under-provisioning
	 */
	 public Repository dataStore=null;
	 
	 public static long totalExecutionTime;
		public static long totalExecTimeWithoutDependencyTransfer;
		public static long  startExecutionTime;


//	 MaxMin MaxMin;
//	 MinMin MinMin;

	protected double useThisProcPower = Double.MAX_VALUE;
	public int completedCount = 0;
	public int jobsNumber = 0;
	VMKeeper[] vms;
	VirtualMachine[] vmsTemp;
	int indexVM=0;
	QueueManager qm;
	public static FileWriter  fileWriter ;
	public static PrintWriter printWriter;
	public Engine(Mapper producer, IaaSService target) throws Exception{
		System.out.println("Engine constructor");
		this.target = target;

//		MaxMin=new MaxMin();
//		MinMin=new MinMin();
		//resetIaaS(target);
		// Collecting the jobs
		 jobs = producer.getAllJobs();
		 jobsNumber=jobs.size();
		//	vm=new VirtualMachine[10];
		 
		// Preparing the repositories with VAs
			repo = target.repositories.get(0);
			va = new VirtualAppliance("test", 30, 0, true, 100000000);
			//va = (VirtualAppliance) repo.contents().iterator().next();
				// actually registering the VA
				repo.registerObject(va);
				// determining the maximum number of CPU cores available in a PM
				for (PhysicalMachine pm : target.machines) {
					double cores = pm.getCapacities().getRequiredCPUs();
					double pp = pm.getCapacities().getRequiredProcessingPower();
					if (cores > maxmachinecores) {
						maxmachinecores = (long) cores;
					}
					if (pp < useThisProcPower) {
						useThisProcPower = pp;
					}
				}
				if (target.machines.size() > maxIaaSmachines) {
					maxIaaSmachines = target.machines.size();
				}
				
				fileWriter = new FileWriter("output.txt");
				printWriter = new PrintWriter(fileWriter);
			
				
				
				
				createVMs();
				 qm = new QueueManager(vms, target);
				executeWorkflowJobs();	

	}
	
	
	
	void createVMs() throws VMManagementException, NetworkException {
		
		Iterator<VMKeeper> it = pooledVMs.iterator();     //!! why did clean up here ans/because less than no of clouds
	/*	while (it.hasNext()) {                             // !! when will get in here?
			VMKeeper curr = it.next();
			if (!curr.isAlive()) {
				it.remove();
			}
		}*/
		int vmpointer = 0;
		vms = new VMKeeper[target.machines.size()];
		it = pooledVMs.iterator();
		//////////////////////////////////////////
		/*	while (it.hasNext() ) {// not understand when will get it
			VMKeeper current = it.next();
			if (current.isFree() ) {
				it.remove();
				vms[vmpointer++] = current;
			}
		}*/
		
		/////////////////////////////
		ResourceConstraints rcForMachine = target.machines.get(0).getCapacities();
	int	pmCores = (int) rcForMachine.getRequiredCPUs();
	double	pmProcessing = rcForMachine.getRequiredProcessingPower();
	long	pmMem = rcForMachine.getRequiredMemory();
	vmsTemp=new VirtualMachine[target.machines.size()]; //target.machines.size()
		for(int i=0;i<target.machines.size();++i) {
		System.out.println(i+"=  "+pmCores+"  "+pmProcessing+"  "+pmMem+"    "+target.repositories.get(0).getInputbw() );
			vmsTemp[i] = target.requestVM(va, new ConstantConstraints(pmCores, pmProcessing,  pmMem), repo, 1)[0];
			 
		}
			for (int k = 0; k < target.machines.size(); k++) {
				VMKeeper newKeeper = new VMKeeper(target, vmsTemp[k], 3600 * 100000);
				newKeeper.setListener(new VMKeeper.ReleaseListener() {

					@Override
					public void released(VMKeeper me) {
						pooledVMs.add(me);
					}
				});
				vms[k] = newKeeper;
				//System.out.println(vms[k].isFree());
				
				
	}
		
		
		}
	
	
	
public void executeWorkflowJobs() throws Exception {
		
		for (WorkflowJob job : jobs.values()){
			final WorkflowJob toprocess = job;
			if(toprocess.ready && toprocess.startedProcessing==false ) {
				
		
				
		
				addJobToQueue(toprocess);
	
			}
	}
				
	}
	

	@Override
	public void stateChanged(WorkflowJob job, StateJob oldState, StateJob newState) {
		// TODO Auto-generated method stub
		

		if (newState.equals(WorkflowJob.StateJob.COMPLETED)) {
			///System.out.println("ID="+job.jobId);
			//WorkflowJob job = jobs.get(id);
			if (job != null)
			{
				// Get a list of the children jobs
				for (String child_id : job.childrenJobs) 
				{
					// Get a list of the jobs depending on a particular output file
					WorkflowJob childJob = jobs.get(child_id);
					// Remove this depending parent job
					childJob.removeParent(job.jobId);
					if(childJob.ready) addJobToQueue(childJob);
				}
				//jobs.remove(job.jobId); // delete the completed job from the list
			}
			completedCount++;
			// count number of VMs
			System.out.println("jobsNumber="+jobsNumber+"    "+"completedCount="+completedCount);
			if(completedCount==jobsNumber) {
				printWriter.close();
//				System.out.println("The total workflow execution time Without Dependency Transfer is "+totalExecTimeWithoutDependencyTransfer/1000);
			}
			
			
				
				
		}
						
			}
	
	void addJobToQueue(WorkflowJob toprocess) {
		qm.add(toprocess);
		toprocess.startedProcessing();
		toprocess.subscribeStateChange(this);
		toprocess.setReleasetimeSecs(Timed.getFireCount());
		System.out.println("start release="+Timed.getFireCount()+"  "+toprocess.jobId);
		
	}
	
	
				
	


}
