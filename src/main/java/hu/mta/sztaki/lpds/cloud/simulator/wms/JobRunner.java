package hu.mta.sztaki.lpds.cloud.simulator.wms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.VMKeeper;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;

public class JobRunner extends ConsumptionEventAdapter implements VirtualMachine.StateChange {

	private VMKeeper vmKeeper;
	private VirtualMachine vm;
	WorkflowJob toRun;
	int loc;
	//ArrayList<Long> statusReports;
	Repository dataFrom, dataTo;
	int len=0;
	protected Long[] inFiles;
	protected Long[] outFiles;
	int inFilesIndex=0;
	int outFilesIndex=0;
	protected boolean transferInFiles=false;
	protected boolean executeJob=false;
	protected boolean transferOutFiles=false;

	public long singleJobExecTime;



	//dataTo = vm.getResourceAllocation().getHost().localDisk;
	public JobRunner( VMKeeper vmKeeper, WorkflowJob toprocess, Repository dataStorage) throws NetworkException, IOException {
		//String fileContent = "Hello Learner !! Welcome to howtodoinjava.com.";


	   // fileWriter.close();
		this.vmKeeper = vmKeeper;
		this.toRun = toprocess;
		//System.out.println("KKKKKKKKKKKKKKKK="+toprocess.parentJobs.size());
		this.dataFrom = dataStorage;
		this.vm=this.vmKeeper.acquire();
		this.toRun.pmExecutedJob=vm.getResourceAllocation().getHost();
		//System.out.println("Host================"+vm.getResourceAllocation().getHost());
		//System.err.println(this.vm.getState());
	//	System.out.println("Physical Machine="+this.vm.getResourceAllocation().allocated);
		System.out.println("JobRunner constructor");
		len=toprocess.inFiles.values().size();
		this.inFiles=toprocess.inFiles.values().toArray(new Long[len]);
		len=toprocess.outFiles.values().size();
		this.outFiles=toprocess.outFiles.values().toArray(new Long[len]);

		if (VirtualMachine.State.RUNNING.equals(vm.getState())) {
			this.vm.subscribeStateChange(this);
			startProcess();


	}  else this.vm.subscribeStateChange(this);


	}

	@Override
	public void stateChanged(VirtualMachine vm, State oldState, State newState) {
		// TODO Auto-generated method stub
		if (newState.equals(VirtualMachine.State.RUNNING)) {
		//	System.out.println("Running="+toRun.jobId+"   "+Timed.getFireCount());
			//System.out.println("getResourceAllocation="+vm.getResourceAllocation());

			Engine.startExecutionTime=Timed.getFireCount();
			startProcess();
		}
	}

	@Override
	public void conComplete() {
		super.conComplete();
		//statusReports.add(Timed.getFireCount());
		//loc++;
		if(transferInFiles) {
		try {
			transferInFiles();
		} catch (NetworkException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}

		else if(transferOutFiles) {

			try {

				transferOutFiles();
			} catch (NetworkException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}


		else if(executeJob) {
			submit();

		}
		else {
			System.out.println("Completed="+ toRun.jobId+"  "+Timed.getFireCount());
			  Engine.printWriter.println("Completed="+ toRun.jobId+"  "+Timed.getFireCount());
			singleJobExecTime=Timed.getFireCount()-singleJobExecTime;
			System.out.println("Completed"+ toRun.jobId+" = "+singleJobExecTime);
			Engine.totalExecTimeWithoutDependencyTransfer+=singleJobExecTime;
			Engine.totalExecutionTime=Timed.getFireCount();
		//	System.out.println("toRun="+toRun.jobId+"  len="+len);
			releaseVMset();
			toRun.complete();
			//toRun=null;

		}

	}

	void transferInFiles() throws NetworkException {



		for(String parentId:toRun.parentJobswithDependency.keySet() ) {


				//WorkflowJob parentJob=
				List<WorkflowJob> parentJobs = new ArrayList<WorkflowJob>(toRun.parentJobsforTransfer);
				WorkflowJob parentJob = null;
				 for (int j=0;j<parentJobs.size();++j) {
				if(	 parentJobs.get(j).jobId==parentId) {
					 parentJob=parentJobs.get(j);
					 break;
				}


				 }

				 if (parentJob!=null && this.vm.getResourceAllocation().getHost()==parentJob.pmExecutedJob) {

					 System.out.println(toRun.jobId+"=Zero Transfer"+"    "+toRun.parentJobswithDependency.size());
					 Engine.printWriter.println(toRun.jobId+"=Zero Transfer"+"    "+toRun.parentJobswithDependency.size());
					 toRun.parentJobswithDependency.remove(parentId);

					 //System.out.println(toRun.jobId+"oooooookkkkkk");
						if(toRun.parentJobswithDependency.isEmpty()) {  transferInFiles=false; submit(); break;}
					//System.out.println(toRun.jobId+"=Convert");
						else	transferInFiles();
						//System.out.println("toRun="+toRun.jobId+"  len="+"transferInFiles"+"  "+Timed.getFireCount());

				 }

				 else {
					 for(int i=0;i<toRun.parentJobswithDependency.get(parentId).size();++i) {
					 if(parentId=="Central-Storage") {

						 long size=0;
							if (toRun.parentJobswithDependency.get(parentId).get(i)<=0) size=1;
							else size=toRun.parentJobswithDependency.get(parentId).get(i);

						 NetworkNode.initTransfer(size, ResourceConsumption.unlimitedProcessing, dataFrom, dataTo, this);//toRun.parentJobswithDependency.get(parentId).get(i)
							inFilesIndex++;
							toRun.parentJobswithDependency.get(parentId).remove(i);
						    if(toRun.parentJobswithDependency.get(parentId).size()==0)toRun.parentJobswithDependency.remove(parentId);
							if(toRun.parentJobswithDependency.isEmpty()) { executeJob=true; transferInFiles=false;}
							//System.out.println("toRun="+toRun.jobId+"  len="+"transferInFiles"+"  "+Timed.getFireCount());
							 System.out.println(toRun.jobId+"=Transfer-from-Central-Storage"+"  "+Timed.getFireCount());
							 Engine.printWriter.println(toRun.jobId+"=Transfer-from-Central-Storage"+"  "+Timed.getFireCount());
							 break;

					 }
					 else {
						 long size=0;
							if (toRun.parentJobswithDependency.get(parentId).get(i)<=0) size=1;
							else size=toRun.parentJobswithDependency.get(parentId).get(i);
					//	 System.out.println("NOOOOOOOOOOOOTE="+toRun.parentJobswithDependency.get(parentId).get(i)+"    "+parentJob.pmExecutedJob);
						 NetworkNode.initTransfer(size, ResourceConsumption.unlimitedProcessing, parentJob.pmExecutedJob.localDisk, dataTo, this);
							toRun.parentJobswithDependency.get(parentId).remove(i);
						    inFilesIndex++;
						    if(toRun.parentJobswithDependency.get(parentId).size()==0)toRun.parentJobswithDependency.remove(parentId);
							if(toRun.parentJobswithDependency.isEmpty()) { executeJob=true; transferInFiles=false;}
						//	System.out.println("toRun="+toRun.jobId+"  len="+"transferInFiles"+"  "+Timed.getFireCount());
							System.out.println(toRun.jobId+"=Transfer-between-PMs" +"  "+Timed.getFireCount());
							Engine.printWriter.println(toRun.jobId+"=Transfer-between-PMs" +"  "+Timed.getFireCount());
							break;
					 }

				//System.out.println(parentId +"   "+ toRun.parentJobswithDependency.get(parentId).get(i) );
			}

		}
		break;
		}

	}


	void transferOutFiles() throws NetworkException {
		 long size=0;
			if (outFiles[outFilesIndex]<=0) size=1;
			else size=outFiles[outFilesIndex];
		NetworkNode.initTransfer(size, ResourceConsumption.unlimitedProcessing, dataTo,dataFrom , this);
		outFilesIndex++;
		if(outFilesIndex==outFiles.length) {  transferOutFiles=false;}
		System.out.println("toRun="+toRun.jobId+"  len="+"transferOutFiles"+"  "+Timed.getFireCount());
		Engine.printWriter.println("toRun="+toRun.jobId+"  len="+"transferOutFiles"+"  "+Timed.getFireCount());
	}




	void submit()  {

			//this is important I will need it to transfer data between VMs

			try {
				double time=0;
				if (toRun.getExectimeSeconds()<=0) time=0.1;
				else time=toRun.getExectimeSeconds();
						vm.newComputeTask(time, ResourceConsumption.unlimitedProcessing, this);
					//completed=true;
						System.out.println("toRun="+toRun.jobId+"  len="+"executeJob"+"  "+Timed.getFireCount());
						Engine.printWriter.println("toRun="+toRun.jobId+"  len="+"executeJob"+"  "+Timed.getFireCount());
						singleJobExecTime=Timed.getFireCount();
						if (toRun.childrenJobs.size()==0) transferOutFiles=true;
						executeJob=false;

				} catch (NetworkException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

	private void releaseVMset() {
			this.vm.unsubscribeStateChange(this);
			vmKeeper.release(this.vm);
			vmKeeper = null;
			vm = null;

	}

	public void startProcess() {
		System.out.println("Running="+toRun.jobId+"   "+Timed.getFireCount());
		Engine.printWriter.println("Running="+toRun.jobId+"   "+Timed.getFireCount());
		//System.out.println("getResourceAllocation="+vm.getResourceAllocation());

		dataTo = vm.getResourceAllocation().getHost().localDisk;
		if (inFiles.length>0) {
			//System.out.print("No");
			transferInFiles=true;
			try {
				transferInFiles();
			} catch (NetworkException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			//System.out.print("OK");
			submit();
			//executeJob=true;
		}
	}


}
