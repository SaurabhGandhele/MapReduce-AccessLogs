/*

## Authors

## Saurabh Gandhele (smg6512@rit.edu)
## Surabhi Marathe  (srm6226@rit.edu)
## Saurabh Wani     (saw4058@rit.edu)

## This program will read the access log source files provided as input
## and display the top 15 occuring IP addresses with their respective accounts accessed. 
## By applying Map Reduce functions provided in the MapReduce.class file provided by Professor.

*/

import java.util.*; 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException; 
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/* extend the MapReduce class to implement the given abstract methods */
public class Project3SourceCode extends MapReduce {
	
  public static Pattern IPaddressregex = Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})"); // regular expression to find IP address
	
  public static HashMap<String,ArrayList<String>> IPaccount=new HashMap<String,ArrayList<String>>();  // store the IP address and accounts it accessed	
    
  /* mapper function to iteratively read every line and call emit_intermediate() function to process the IP addresses and list of accounts accessed */ 
   
	public void mapper(java.lang.String line) 
	{
   String files[]=line.split(" ");    // split the file names
   
   for(int i=0;i<files.length;i++)
   {
	   File logfile = new File("//usr//local//pub//large_log_files//"+files[i]);    // locate the file in the given path
     try 
      {
       FileReader readfile = new FileReader(logfile);
       BufferedReader buffread = new BufferedReader(readfile);    // read the access log file
	      int linenum=0;
        String temp;
        while((temp=buffread.readLine())!=null)   // iterate till file end reading line by line
          {
    			   //String IPaddress = "";
             String[] words = temp.split(" ");  
             Matcher checkIP = IPaddressregex.matcher(temp);  // check and match the IP address regular expression
		 
		       if(checkIP.find())  // if IP address is found
           {
            String IPaddress = checkIP.group();       // store the IP address          
					 //IPaddress=words[0];
         	 
            if(temp.contains("~"))  // check if the account is present by locating the ~
					   {
						   int tildeind = temp.indexOf('~');   // find the respective index
						   int slashind = temp.indexOf('/', tildeind);
               int spaceind = temp.indexOf(' ', tildeind);      // index of space           
						   String acctname = "";
            
                try{
                
						       if(!temp.substring(tildeind+1, slashind).contains(" "))
							       acctname=temp.substring(tildeind+1, slashind);  // store the account name having backward slash at end
  						     else
							       acctname=temp.substring(tildeind+1, spaceind);  // store the account name having space at end
                                                                                         
				        }catch(Exception e)  // catch exception if some account is not present or accessed
  						     {
							      //e.printStackTrace();
  							     //System.out.println(temp);
						       }
                                                                
                 if(!(IPaccount.containsKey(IPaddress)))    // check if the IP address is already present
                 {
                   ArrayList<String> inter = new ArrayList<String>();    // create a new list
                   inter.add(acctname);    // add the IP address in the list
                   IPaccount.put(IPaddress, inter);     // store the IP address and its account ina a local data structure
                    emit_intermediate(IPaddress, acctname);    // call to emit_intermediate function to count and process the IP address and its account
                 }        
                                                      
                 if(IPaccount.containsKey(IPaddress))  // if the IP address is already present then add the new account accessed
                 {
                   if(!(IPaccount.get(IPaddress).contains(acctname)))
                   {
                     IPaccount.get(IPaddress).add(acctname);  // add the new account with the old one
                    //IPaccount.put(IPaddress,inter); 
                    emit_intermediate(IPaddress,acctname);  //  // call to emit_intermediate function to count and process the IP address and its account
                    }
                   else
                     {
                        // emit_intermediate(IPaddress,acctname);
                     }
                  } 
                }
					     }          
			       }
		      } catch (FileNotFoundException e) {    // file not found exception
			    // TODO Auto-generated catch block
			    e.printStackTrace();
	    } catch (IOException e) {    // IO exception
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
	
 /* reducer function to iteratively read every IP address and account accessed from mapper to emit it in a tree map structure*/
 
	public void reducer(java.lang.String key, java.util.LinkedList<java.lang.String> value)
	{
		 for(int i=0;i<value.size();i++) // check if IP address and its account accessed are occuring in pair and does not contain space
		 {
      if(value.get(i).contains(" "))
      {
        String access = value.get(i);
      }
     }
    
    for(int i=0;i<value.size();i++) // emit every account accessed by each unique IP address
		{
			emit(key, value.get(i));
		}
 	
	}
	
 /* evaluateresult function to display the top IP addresses and their accounts accessed */
 
	public static void evaluateresult(TreeMap<String, LinkedList<String>> result)
  {
    		
    int finalcnt=1;
	    
   int min = -1;
   
		while(finalcnt<=15 && result.size() > 0)
		{
			String address="";
			min=-1;
		  for(String keyIP : result.keySet())   // get the size of list having accounts
		    {
			
			    if(result.get(keyIP).size() > min)  // find the most accessed accounts IP address
			    {
				    min=result.get(keyIP).size();
				     address = keyIP;
			    }
		    }
   
        System.out.println();
		    System.out.println("#"+finalcnt+" ocurring address : "+address);    // display the key IP address
        System.out.println();
		    System.out.print("              ");
	
  	    for(int i=0;i<result.get(address).size();i++)
		    {
			    if((i+1)==result.get(address).size())
          System.out.print(result.get(address).get(i)+" ");    // display the respective accounts
          else
          System.out.print(result.get(address).get(i)+", ");
      
		    }
   
  		  result.remove(address);
		    finalcnt++;    // increment the tracker count
		    System.out.println();
		    System.out.println();
   
     }
  }
 
 /* main function */
 
	public static void main(String args[])
	{
		Project3SourceCode pcode = new Project3SourceCode();   // create object to access the abstract MapReduce methods
		
    String sendto="";
		
    for(int i=0;i<args.length;i++)
     {
      sendto=sendto + args[i]+" ";  // read all the file names provided as arguments
     }
     
     pcode.execute(sendto);    // call execute function of MapReduce with the file names
     
     TreeMap<String, LinkedList<String>> result = new TreeMap<String, LinkedList<String>>();
		
    result=pcode.getResults();  // call get results to access all the stored IP address and their respective accounts
		
    evaluateresult(result);
    
  }
}