package hu.mta.sztaki.lpds.cloud.simulator.wms;

import java.util.Calendar;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.examples.util.DCCreation;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.pmscheduling.AlwaysOnMachines;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.pmscheduling.MultiPMController;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.pmscheduling.SchedulingDependentMachines;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.FirstFitScheduler;

public class SchedulerManager {
public static void main(String[] args) throws Exception {
		

		// The preparation of the clouds
		IaaSService cloud;
		int numofCores = 2;
		int numofNodes = 3;
		
		cloud=DCCreation.createDataCentre(FirstFitScheduler.class, AlwaysOnMachines.class, numofNodes, numofCores);
		Timed.simulateUntilLastEvent();
		//-2000-005  CYBERSHAKE.n.1000.19.dax
		Mapper producer =new  Mapper("/home/hairbui76/Desktop/github/Dissect-cf/CyberShake_30.xml");
		//Timed.simulateUntilLastEvent(); 28814.33
		Engine engine=new Engine(producer, cloud);
		Timed.simulateUntilLastEvent();
		long beforeSimu = Calendar.getInstance().getTimeInMillis();
		System.out.println("Event-handling");
		Timed.simulateUntilLastEvent();
		long afterSimu = Calendar.getInstance().getTimeInMillis();
		long duration = afterSimu - beforeSimu;
		System.err.println("Simulation terminated " + afterSimu + " (took " + duration + "ms in realtime)");
		System.err.println("Current simulation time: " + Timed.getFireCount());
		System.out.println("The workflow execution time is "+ (double)(engine.totalExecutionTime-engine.startExecutionTime)/1000+" s");

		
		long vmcount = 0;
		
			for (PhysicalMachine pm : cloud.machines) {
				vmcount += pm.getCompletedVMs();
			}
			System.out.println("vmcount="+vmcount);
		
		System.err.println("Performance: " + (((double) vmcount) / (engine.totalExecutionTime-engine.startExecutionTime)) + " VMs/ms ");

//	for (int i=0; i<cloud.machines.size();++i)
		//	System.out.println(cloud.machines.get(i).getCompletedVMs());

	//	WorkflowAnalysingLevel a=new WorkflowAnalysingLevel(producer);

		
}

}
