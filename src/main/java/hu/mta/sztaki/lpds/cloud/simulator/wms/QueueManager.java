/*
 *  ========================================================================
 *  DISSECT-CF Examples
 *  ========================================================================
 *
 *  This file is part of DISSECT-CF Examples.
 *
 *  DISSECT-CF Examples is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published
 *  by the Free Software Foundation, either version 3 of the License, or (at
 *  your option) any later version.
 *
 *  DISSECT-CF Examples is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with DISSECT-CF Examples.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  (C) Copyright 2019, Gabor Kecskemeti (g.kecskemeti@ljmu.ac.uk)
 */
package hu.mta.sztaki.lpds.cloud.simulator.wms;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.VMKeeper;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.wms.WorkflowJob;

/**
 * Allows a jobs to be queued if it was not possible to find an acceptable VM
 * for it at a given moment. This class will periodically retry starting the job
 * with the help of a specified job launcher.
 *
 * This class does not provide any queue reordering features (e.g., job
 * priorities, and other QoS metrics)
 *
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2019"
 */
class QueueManager extends Timed {

	static ArrayList<VirtualMachine> busyVMs=new ArrayList<VirtualMachine>();

	VMKeeper[] vms;

	private Repository repo;
	/**
	 * The virtual infrastructure this launcher will target with its jobs.
	 */
	//private final VirtualInfrastructure vi;
	/**
	 * The object which will receive the progress updates about the various job
	 * related activities.
	 */
	//private final Progress progress;

	/**
	 * The job launcher to with which we can do the job re-submissions
	 */
	//private final JobLauncher launcher;

	/**
	 * The actual queue. Key: application kind. Value: the list of jobs waiting
	 * there.
	 */
	private final HashMap<String, ArrayDeque<WorkflowJob>> queued = new HashMap<String, ArrayDeque<WorkflowJob>>();

	/**
	 * Saves the input parameter
	 *
	 * @param launcher The job scheduling mechanism to be used when a job retry is
	 *                 needed.
	 */
	public QueueManager(VMKeeper[] vms, IaaSService cloud) {
		this.vms=vms;
		repo = cloud.repositories.get(0);
	}

	/**
	 * Allows queueing a job, makes sure the queue manager receives updates in every
	 * ten seconds if there are any jobs on the queue.
	 *
	 * @param j The job to be queued
	 */
	public void add(final WorkflowJob j) {
		ArrayDeque<WorkflowJob> q = queued.get(j.executable);
		if (q == null) {
			q = new ArrayDeque<WorkflowJob>();
			queued.put(j.executable, q);
		}
		q.add(j);
		if (!isSubscribed()) {
			subscribe(1);
		}
	}

	/**
	 * The queue management algorithm will attempt to launch a job if it is first in
	 * its executable's queue.
	 */
	@Override
	public void tick(final long fires) {
		final Iterator<String> kindIter = queued.keySet().iterator();
		queueChangeLoop: while (kindIter.hasNext()) {
		//	System.err.println("kindIter="+kindIter);

			// The queue for a specific kind of executable
			final ArrayDeque<WorkflowJob> q = queued.get(kindIter.next());
			do {
				//System.err.println("q="+q.size());
		//		System.out.println("in QueuManager in Tick="+q.peekFirst().jobId+"  "+Timed.getFireCount());
				// Launch the current head of the queue
				VMKeeper a=GetVM();
					if (a==null)
						continue queueChangeLoop;
					// Removes the head as we were successful in dispatching it to the
					// infrastructure
					else {
					try {
						new JobRunner(a,q.peekFirst(),repo);
						System.out.println("toprocess="+q.peekFirst().jobId+"  "+q.peekFirst().getExectimeSecs()+"  "+Timed.getFireCount()+ "   VM");

					} catch (NetworkException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("in QueuManager in Tick="+q.peekFirst().jobId+" deleted ");
					q.pollFirst();

					}
			} while (!q.isEmpty());
			kindIter.remove();
		}
		if (queued.size() == 0) {
			unsubscribe();
		}
	}

	VMKeeper GetVM() {
		for (int k = 0; k < vms.length; k++)
			if (vms[k].isFree()) return vms[k];

		return null;

	}
}