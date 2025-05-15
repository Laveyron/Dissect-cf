package hu.mta.sztaki.lpds.cloud.simulator.wms;

import java.io.*;
import java.util.*;

import org.dom4j.*;
import org.dom4j.io.SAXReader;


//import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;

public class Mapper {
	
	public File file;
	public HashMap<String, WorkflowJob> jobs;
	
	public Document document;
	
	public Mapper(String fileName) {
		
		
		jobs = new LinkedHashMap<String, WorkflowJob>();
    //	JsonFile obj;

		file = new File(fileName);
        if (fileName.endsWith(".xml") || fileName.endsWith(".dax")) 
		parseWorkflow(file);
       // else if (fileName.endsWith(".json"))
        //	obj= new JsonFile(file,  jobs);
        else System.err.println("The file extension is not XML ");
		System.out.println("Mapper constructor");

		jobsDependencyFiles();
		prepareLevels();
	}


	void jobsDependencyFiles() {
		List<String> transferredFiles = null;
		for (WorkflowJob job:jobs.values()) {
			//System.out.println(job.jobId);
			if (job.parentJobs.size()!=0) {
				//System.out.println("step");
		List<String> list = new ArrayList<String>(job.parentJobs);
		transferredFiles = new ArrayList<String>();
		 for (int i=0;i<list.size();++i) {	
				//System.out.println("Itreator="+list.get(i));
			 WorkflowJob parentJob=jobs.get(list.get(i));
			 List<Long> listFilesize = new ArrayList<Long>();
			 job.parentJobswithDependency.put(parentJob.jobId, listFilesize);
			 for (String fileName:job.inFiles.keySet()) {				 
				 if(parentJob.outFiles.containsKey(fileName)) 	{
					// System.out.println(fileName+"   "+job.inFiles.get(fileName));
					 job.parentJobswithDependency.get(parentJob.jobId).add(job.inFiles.get(fileName));
					 transferredFiles.add(fileName);
				 }
				 
				
			 }
		 }
		
		 
		
	}
			
			 List<Long> listFileCentral = new ArrayList<Long>();
			 for (String fileName:job.inFiles.keySet()) {
				 if(transferredFiles!=null && transferredFiles.contains(fileName)) continue;
				 else {
					 
					 job.parentJobswithDependency.put("Central-Storage", listFileCentral);
					 job.parentJobswithDependency.get("Central-Storage").add(job.inFiles.get(fileName)); 
					 
				 }

			 }
			
		}
		}
	
	
	
	void parseWorkflow(File fileName) {
		try {
         SAXReader reader = new SAXReader();
		document = reader.read(file);
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		List<Element> jobs = document.getRootElement().elements("job");
		List<Element> children = document.getRootElement().elements("child");
		//System.out.println(jobs.size());
		//System.out.println(children.size());
		
		
		for(Element job : jobs) 
		{
			
			String id = job.attribute("id").getValue();
			String name = job.attribute("name").getValue();
			String runtime = job.attribute("runtime").getValue();
		//	System.err.println("IDD="+id);
			prepareJob(job);
			prepareFilesJob(job);
		//	System.out.println(id+"   "+name+"    "+runtime);
		}
		System.err.println("children="+children.size());
		for(Element child : children) 
		{
			prepareChild(child);
		}

	}
	
	// This method to read input files and output files of each job in the workflow

	public  void prepareFilesJob(Element element) {
		
		String id = element.attribute("id").getValue();

//////////////////////////////////////////
//		System.out.println(id);
		for ( Iterator iter = element.elementIterator( "uses" ); iter.hasNext(); ) 
		{

			Element file = (Element) iter.next();
			if (file.attribute("link").getValue().equals("input"))
			{

				jobs.get(id).inFiles.put(file.attribute("file").getValue(),Math.abs(Long.parseLong(file.attribute("size").getValue())));
				//	System.out.println("IN: "+file.attribute("file").getValue()+"      "+Long.parseLong(file.attribute("size").getValue()));		
			}
			else
			{
				jobs.get(id).outFiles.put(file.attribute("file").getValue(),Math.abs(Long.parseLong(file.attribute("size").getValue())));
			//		System.out.println("OUT:"+file.attribute("file").getValue()+"      "+Long.parseLong(file.attribute("size").getValue()));
			}
		}

		
	}
	
	// This method to read id, name and runtime of each job in the workflow

	public  void prepareJob(Element element)
	{
		String id = element.attribute("id").getValue();
		String name = element.attribute("name").getValue();
		double runtime=(Double.parseDouble(element.attribute("runtime").getValue()));
		//	System.err.println("runtime="+runtime);
	
		WorkflowJob job = new WorkflowJob(id,0,0,Math.abs(runtime),0,0,0,null,null,"X",null,0,name);	
		
		jobs.put(id, job);
	}
	
	// This method to make a list for each job about its parents and children in the workflow

	
	public  void prepareChild(Element child)
	{
		String child_id = child.attribute("ref").getValue();
		List<Element> parents = child.elements("parent");
		
		for (Element parent: parents)
		{
			String parent_id = parent.attribute("ref").getValue();
			jobs.get(child_id).addParent(parent_id);
			
			jobs.get(child_id).addParentJob(jobs.get(parent_id));
			jobs.get(parent_id).addChild(child_id);
			
		//	if (jobs.get(parent_id).getLevel()>max) max=jobs.get(parent_id).getLevel();
			
		}
	//	jobs.get(child_id).setLevel(max+1);
	//	System.err.println(child_id+"    "+ (max+1));
	}
	
	public HashMap<String, WorkflowJob> getAllJobs() {
		return jobs;
	}
	
	private void prepareLevels() {
		
		for (WorkflowJob job:jobs.values()) 
			if (job.parentJobs.size()==0) job.setLevel(1);
		boolean flag=true;
		while(flag) {
			flag=false;
			for (WorkflowJob job:jobs.values()) {
				if (job.parentJobs.size()==0) continue;
				int max=0; 
				boolean sign=true;
				
			for(String parent:job.parentJobs) {
				if(jobs.get(parent).getLevel()<0) { flag=true; sign=false; break;  }
				if (jobs.get(parent).getLevel()>max) max=jobs.get(parent).getLevel();
				
			}
			if (sign==true) job.setLevel(max+1);
			}
		}
		
	}
	
	private void computeLevel() {
		for (WorkflowJob job : jobs.values()){
			if (job.parentJobs.size()==0) job.setLevel(1);
	//		System.err.println(job.getId()+"   "+job.getExectimeSeconds());
		   
		}
		for (WorkflowJob job : jobs.values()){
			if (job.parentJobs.size()!=0) continue;
			   for(String child : job.childrenJobs) {
				   
				   WorkflowJob childJob=jobs.get(child);
				   if(childJob.getLevel()<0) {
					   jobLevel(childJob);
			//	   System.out.println("Invoke="+parentJob.jobId);	
				 //  break;
				   }
				   
			   }
		}
	}
	void jobLevel(WorkflowJob j) {
		int max=0;
		for(String parent : j.parentJobs) {		
			if (jobs.get(parent).getLevel()>max) max=jobs.get(parent).getLevel();
	}
		j.setLevel(max+1);
		
		if(j.parentJobs.size()>0) {
			  for(String child : j.childrenJobs) {
				   
				   WorkflowJob childJob=jobs.get(child);
				   jobLevel(childJob);  
			  }
		  }
}
}