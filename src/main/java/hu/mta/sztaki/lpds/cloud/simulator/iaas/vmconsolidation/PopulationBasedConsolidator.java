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
 *  (C) Copyright 2017, Gabor Kecskemeti (g.kecskemeti@ljmu.ac.uk)
 */
package hu.mta.sztaki.lpds.cloud.simulator.iaas.vmconsolidation;

import java.lang.reflect.Field;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmconsolidation.model.GenHelper;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmconsolidation.model.InfrastructureModel;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmconsolidation.model.InfrastructureModel.Improver;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmconsolidation.model.PreserveAllocations;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmconsolidation.model.RandomVMassigner;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmconsolidation.model.improver.FirstFitBFD;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmconsolidation.model.improver.NonImprover;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmconsolidation.model.improver.SimpleConsImprover;
import it.unimi.dsi.util.XoShiRo256PlusRandom;

/**
 * This class is to be sub-classed by all workload consolidators that use the
 * Solution class. It allows the loading of the mutation probability
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2017"
 */
public abstract class PopulationBasedConsolidator<T extends InfrastructureModel> extends ModelBasedConsolidator {

	protected int randomCreations;
	protected int unchangedCreations;
	protected int firstFitCreations;
	/** Terminate the GA after this many generations */
	protected int nrIterations;
	/** Population for the GA, consisting of solutions=individuals */
	protected T[] population;
	private int popFillIndex;
	/** For generating random numbers */
	static public XoShiRo256PlusRandom random;
	/**
	 * Controls whether new solutions (created by mutation or recombination) should
	 * be improved with a local search
	 */
	protected InfrastructureModel.Improver localSearch;

	protected T input;

	/** True if at least one solution has improved during the current iteration */
	protected boolean improved;

	public static final FitCompare baseComp = InfrastructureModel::isBetterThan;

	protected interface FitCompare {
		boolean isBetterThan(InfrastructureModel a, InfrastructureModel b);
	}

	public PopulationBasedConsolidator(final IaaSService toConsolidate, final long consFreq) {
		super(toConsolidate, consFreq);
	}

	protected abstract T modelFactory(T input, GenHelper vmAssignment, InfrastructureModel.Improver localsearch);

	protected abstract void createPopArray(int len);

	/**
	 * Initializes the population with the previously determined solutions. After
	 * that the same mapping as existing before consolidation has started is put
	 * inside a solution.
	 */
	protected void initializePopulation(final T input) {
		this.input = input;
		popFillIndex = 0;
		for (int i = 0; i < randomCreations; i++) {
			regSolution(modelFactory(input, RandomVMassigner.globalRandomAssigner, localSearch));
		}
		if (firstFitCreations != 0) {
			produceClonesOf(regSolution(modelFactory(input, PreserveAllocations.singleton, localSearch)),
					firstFitCreations - 1);
		}
		if (unchangedCreations != 0) {
			produceClonesOf(regSolution(modelFactory(input, PreserveAllocations.singleton, NonImprover.singleton)),
					unchangedCreations - 1);
		}
	}

	protected T regSolution(final T toReg) {
		return population[popFillIndex++] = toReg;
	}

	protected void produceClonesOf(final T s0, int clonecount) {
		while (clonecount > 0) {
			regSolution(modelFactory(s0, PreserveAllocations.singleton, NonImprover.singleton));
			clonecount--;
		}
	}

	protected int getPopFillIndex() {
		return popFillIndex;
	}

	@Override
	protected void processProps() {
		random = new XoShiRo256PlusRandom(Long.parseLong(props.getProperty("seed")));
		String impClassName = props.getProperty("improver");
		if (impClassName == null) {
			if (Boolean.parseBoolean(props.getProperty("doLocalSearch1"))) {
				localSearch = FirstFitBFD.singleton;
			} else if (Boolean.parseBoolean(props.getProperty("doLocalSearch2"))) {
				localSearch = SimpleConsImprover.singleton;
			} else {
				localSearch = NonImprover.singleton;
			}
		} else {
			try {
				@SuppressWarnings("rawtypes")
				Class impClass = Class.forName(impClassName);
				Field singletonField = impClass.getField("singleton");
				localSearch = (Improver) singletonField.get(impClass);
				System.err.println("Improver loaded and applied: " + impClassName);
			} catch (ClassNotFoundException e) {
				System.err.println("The configured improver class is not available: " + impClassName);
				System.exit(1);
			} catch (NoSuchFieldException e) {
				System.err
						.println("The configured improver class does not have a singleton data field: " + impClassName);
				System.exit(1);
			} catch (ClassCastException e) {
				System.err.println(
						"The configured improver class's singleton data field is not implementing the Improver interface: "
								+ impClassName);
				System.exit(1);
			} catch (Exception e) {
				// Unknown reason
				e.printStackTrace();
				System.exit(1);
			}
		}
		createPopArray(Integer.parseInt(props.getProperty("populationSize")));
		this.nrIterations = Integer.parseInt(props.getProperty("nrIterations"));
		determineCreations(population.length);
	}

	/**
	 * We have to determine how to fill the population/swarm. At the moment there is
	 * going to be one unchanged solution, size * 0.25 first fit solutions and the
	 * rest of the creations is made randomly.
	 * 
	 * Note that there will be only random creations if the populationSize/swarmSize
	 * is less than three.
	 * 
	 * @param numberOfCreations The swarmSize/populationSize.
	 */
	protected void determineCreations(final int numberOfCreations) {
		// if the populationSize is less than 3, we only use random creations
		if (numberOfCreations < 3) {
			randomCreations = numberOfCreations;
			unchangedCreations = 0;
			firstFitCreations = 0;
		} else if (numberOfCreations == 3) {
			randomCreations = 1;
			unchangedCreations = 1;
			firstFitCreations = 1;
		} else {
			unchangedCreations = 1;
			firstFitCreations = numberOfCreations / 4;
			randomCreations = numberOfCreations - unchangedCreations - firstFitCreations;
		}
	}

	protected boolean checkAndReplace(final T imTest, final int idx) {
		boolean replaced = false;
		if (imTest.isBetterThan(population[idx])) {
			population[idx] = imTest;
			replaced = improved = true;
		}
		return replaced;
	}

	/**
	 * Determine "best" solution (i.e. an infrastructure setup, compared to which
	 * there is no better one
	 */
	protected int findBestSolution(final FitCompare comp) {
		int bestSolution = 0;
		for (int i = 1; i < population.length; i++) {
			if (comp.isBetterThan(population[i], population[bestSolution])) {
				bestSolution = i;
			}
		}
		return bestSolution;
	}

	/**
	 * String representation of the whole population (for debugging purposes).
	 */
	public String populationToString() {
		return popIdxStream().mapToObj(i ->
				(i != 0 ? " " : "") + population[i]
		).collect(Collectors.joining());
	}

	protected abstract void singleIteration();

	protected abstract T transformInput(InfrastructureModel input);

	protected T getBestResult() {
		return population[findBestSolution(baseComp)];
	}

	@Override
	protected InfrastructureModel optimize(final InfrastructureModel input) {
		initializePopulation(transformInput(input));
		improved = true;
		for (int iter = 0; iter < nrIterations && improved; iter++) {
			improved = false;
			singleIteration();
		}
		return getBestResult();
	}

	protected IntStream popIdxStream() {
		return IntStream.range(0,population.length);
	}
}
