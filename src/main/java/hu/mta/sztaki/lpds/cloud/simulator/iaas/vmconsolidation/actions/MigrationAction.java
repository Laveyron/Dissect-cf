/*
 *  ========================================================================
 *  DIScrete event baSed Energy Consumption simulaTor
 *    					             for Clouds and Federations (DISSECT-CF)
 *  ========================================================================
 *
 *  This file is part of DISSECT-CF.
 *
 *  DISSECT-CF is free software: you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or (at
 *  your option) any later version.
 *
 *  DISSECT-CF is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with DISSECT-CF.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  (C) Copyright 2019-20, Gabor Kecskemeti, Rene Ponto, Zoltan Mann
 */
package hu.mta.sztaki.lpds.cloud.simulator.iaas.vmconsolidation.actions;

import java.util.Arrays;
import java.util.EnumSet;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.consolidation.SimpleConsolidator;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmconsolidation.model.ModelVM;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;

/**
 * This class stores actions, that need to commit a migration in the simulator.
 */
public class MigrationAction extends Action implements VirtualMachine.StateChange {

	// Reference to the model of the VM, which needs be migrated
	public final ModelVM mvm;
	private boolean goForMigration = true;

	/**
	 * Constructor for an action which shall migrate a VM inside the simulator.
	 * 
	 * @param vm The reference to the VM which shall be migrated.
	 */
	public MigrationAction(final ModelVM vm) {
		super(Type.MIGRATION);
		this.mvm = vm;
	}

	/**
	 * This method determines the predecessors of this action. A predecessor of a
	 * migration action is a starting action, which starts the target PM of this
	 * action. Furthermore, migrations from our target PM are also considered
	 * predecessors, in order to prohibit temporary overloads of the PM.
	 *
	 * TODO: this needs improvement, as it can currently lead to deadlocks.
	 */
	@Override
	public void determinePredecessors(final Action[] actions) {
		// looking for actions where a PM gets started, that is the target of this
		// migration
		Arrays.stream(actions).forEach(this::singleActionPredecessorCheck);
	}

	private void singleActionPredecessorCheck(Action action) {
		switch (action.type) {
			case START:
				if ((((StartAction) action).pmToStart.hashCode() == mvm.getHostID())) {
					this.addPredecessor(action);
				}
				break;
			case MIGRATION:
				final ModelVM otherVM = ((MigrationAction) action).mvm;
				boolean cancelMigration = false;
				if (otherVM.basedetails.initialHost().hashCode() == mvm.getHostID()) {
					if (otherVM.getHostID() == mvm.basedetails.initialHost().hashCode()) {
						// Cancels circular migrations
						cancelMigration = true;
					} else {
						// Deeper circles should be efficiently detected
//						if (!this.addPredecessor(action)) {
//							cancelMigration = true;
//						}
					}
				}
				if (cancelMigration) {
					goForMigration = false;
					((MigrationAction) action).goForMigration = false;
				}
			default:
		}
	}

	@Override
	public String toString() {
		return super.toString() + " Source:  " + mvm.basedetails.initialHost().toShortString() + " Target: "
				+ mvm.gethostPM().toShortString() + " VM: " + mvm.toShortString();
	}

	public static final EnumSet<VirtualMachine.State> acceptableStatesForMigr = EnumSet.of(VirtualMachine.State.RUNNING,
			VirtualMachine.State.SUSPENDED);

	/**
	 * Method for doing the migration inside the simulator.
	 */
	@Override
	public void execute() {
		if (goForMigration) {
			final PhysicalMachine simSourcePM = mvm.basedetails.initialHost().getPM();
			final PhysicalMachine simTargetPM = mvm.gethostPM().getPM();
			final VirtualMachine simVM = mvm.basedetails.vm();
			if (simTargetPM.isRunning() && acceptableStatesForMigr.contains(simVM.getState())
					&& simVM.getMemSize() <= simTargetPM.freeCapacities.getRequiredMemory()
					&& simVM.getPerTickProcessingPower() <= simTargetPM.freeCapacities.getTotalProcessingPower()
					&& simSourcePM.publicVms.contains(simVM)) {
				simVM.subscribeStateChange(this); // observe the VM which shall be migrated
				try {
					SimpleConsolidator.migrationCount++;
					simSourcePM.migrateVM(simVM, simTargetPM);
					return;
				} catch (VMManagementException | NetworkException e) {
					e.printStackTrace();
				}
			}
		}
		finished();
	}

	/**
	 * The stateChanged-logic, if the VM changes its state to RUNNING after
	 * migrating, then it does not have to be observed any longer.
	 */
	@Override
	public void stateChanged(final VirtualMachine vm, final State oldState, final State newState) {
		if (newState.equals(VirtualMachine.State.RUNNING)) {
			vm.unsubscribeStateChange(this);
			// Logger.getGlobal().info("Migration action finished");
			// Logger.getGlobal().info("Finished at "+Timed.getFireCount()+":
			// "+toString()+", hash="+Integer.toHexString(System.identityHashCode(this)));
			finished();
		}
	}

}
