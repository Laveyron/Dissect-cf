package hu.mta.sztaki.lpds.cloud.simulator.wms;
import java.util.*;

import org.apache.commons.lang3.tuple.Triple;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.notifications.StateDependentEventHandler;


public class WorkflowJob extends Job {

	public String jobId, jobName;	// job id and job name
	private double exectimeSecs;
	private long releasetimeSecs;
	private long queuetimeSecs;
	public HashSet<String> parentJobs= new HashSet<String>();
	public HashSet<WorkflowJob> parentJobsforTransfer= new HashSet<WorkflowJob>();
	public PhysicalMachine pmExecutedJob;
	public HashSet<String> childrenJobs 	= new HashSet<String>();
	public boolean ready, startedProcessing;
	public HashMap<String,Long> inFiles=new HashMap<String, Long>();
	public HashMap<String,Long> outFiles=new HashMap<String, Long>();
	public HashMap<String,List<Long>> parentJobswithDependency=new HashMap<String,List<Long>>();
	List<Long> list = new ArrayList<Long>();
	private final StateDependentEventHandler<StateChange, Triple<WorkflowJob, StateJob, StateJob>> jobStateChangelistenerManager = JobStateChangeNotificationHandler.getHandlerInstance();

	private StateJob currState = StateJob.WAITING;
	public int level;
//	final static Logger logger = Logger.getLogger(WorkflowJob.class);

	/**
	 *
	 * Constructor
	 *
	 */
//String id, String name, double runtime)
	public WorkflowJob(String id, long submit, long queue, double exec, int nprocs, double ppCpu, long ppMem, String user,
			String group, String executable, Job preceding, long delayAfter, String name)
	{
		super(id, submit,  queue,  0 ,  nprocs,  ppCpu,  ppMem,  user,
				 group, executable, preceding,  delayAfter);

		this.jobId    = id;
		this.jobName  = name;
		this.startedProcessing=false;
		this.exectimeSecs=exec;
		this.queuetimeSecs=0;
		this.releasetimeSecs=0;
		//this.jobRuntime   = runtime;
		// By default this job is ready to go, unless we find out that it has parents!
		ready = true;
		level=-1;
	//	logger.debug(jobXML);
	}


	public double getExectimeSeconds() {
		return exectimeSecs;
	}


	public void setReleasetimeSecs(long releasetimeSecs) {
		 this.releasetimeSecs=releasetimeSecs;
	}
	public long  getReleasetimeSecs() {
		 return releasetimeSecs;
	}

	public long getQueuetimeSecs() {
		return queuetimeSecs;
	}
	public void setQueuetimeSecs(long queuetimeSecs) {
		this.queuetimeSecs=queuetimeSecs;
	}

	/**
	 *
	 * Add a child to the job
	 *
	 */

	public void addChild(String child_id)
	{
		childrenJobs.add(child_id);
	}

	/**
	 *
	 * Add a parent to the job
	 *
	 */

	public void addParent(String parent_id)
	{
		parentJobs.add(parent_id);

		ready = false;
	}

	public void addParentJob( WorkflowJob parent_job) {
		parentJobsforTransfer.add(parent_job);
	}

	/**
	 *
	 * Remove a parent from the job
	 *
	 */

	public void removeParent(String parent_id)
	{
		parentJobs.remove(parent_id);
		if (parentJobs.isEmpty())
		{
			ready = true;
		}
	}

	/**
	 * Records the duration this job spent in the queue.
	 */
	public void started() {
		setRealqueueTime(Timed.getFireCount() / 1000 - getSubmittimeSecs());
	}

	/**
	 * Records the duration this job was executed for.
	 */
	public void completed() {
		setRan(true);
		setRealstopTime(Timed.getFireCount() / 1000 - getSubmittimeSecs());
	}

	public void complete() {

		setState(StateJob.COMPLETED);
	}

	public void startedProcessing() {
		this.startedProcessing=true;
		setState(StateJob.STARTED);
	}


	public static enum StateJob {
	WAITING,
	STARTED,
	COMPLETED

	};
	public interface StateChange {

		void stateChanged(WorkflowJob job, StateJob oldState, StateJob newState);
	}

	private void setState(final StateJob newstate) {
		final StateJob oldState = currState;
		currState = newstate;
		jobStateChangelistenerManager.notifyListeners(Triple.of(this, oldState, newstate));
	}

	public void subscribeStateChange(final StateChange consumer) {
		jobStateChangelistenerManager.subscribeToEvents(consumer);
	}

	/**
	 * Use this function to be no longer notified about state changes on this VM
	 *
	 * @param consumer
	 *            the party who previously received notifications
	 */
	public void unsubscribeStateChange(final StateChange consumer) {
		jobStateChangelistenerManager.unsubscribeFromEvents(consumer);
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int lev) {
		this.level=lev;
	}
}


