<?php
require_once('funcs.php');

ini_set('soap.wsdl_cache_enabled',0);	// Disable caching in PHP
$PhpWsdlAutoRun=true;			// With this global variable PhpWsdl will autorun in quick mode, too
require_once('class.phpwsdl.php');

// In quick mode you can specify the class filename(s) of your webservice 
// optional parameter, if required.

class Pipeline{
	/**
	 * startWorkflow..
	 *
         * @param string $inputparam parameter file 
         * @param string $defaultparam parameter file 
	 * @param string $username username
	 * @param string $workflow workflow name
	 * @param string $wkey key to run previously run workflows
	 * @param string $outdir output directory in the cluster
	 * @param string $services the number of services will be run on this workflow
         * 
	 * @return string Response is a new random key if it is a new run otherwise old key.
	 */
	public function startWorkflow($inputparam, $defaultparam, $username, $workflow, $wkey, $outdir, $services ){
		$myClass = new funcs(); 
                
                $status = "exist";
                if($wkey=='' || $wkey=='start')
                {
 		   $wkey = $myClass->getKey();
                   $status = "new";
                }
		$ret=$myClass->startWorkflow($inputparam, $defaultparam, $username, $workflow, $wkey, $status, $outdir, $services);
                if(eregi("^ERROR", $ret))
                  {
                    return $ret;
                  }
		return $wkey;
	}
 
	/**
	 * Run a service in the galaxy machine or cluster
	 * 
	 * @param string $servicename service name that is going to be run
	 * @param string $wkey a key for the run
	 * @param string $command and parameters for the service separated with |
	 * @return string Response
	 */
	public function startService( $service, $wkey, $command){
		$myClass = new funcs();
		#Check if the job is started
		$result = $myClass->checkStatus($service, $wkey); 
		if ( $result == "START") # Job hasn't started yet 
		{
		     $result=$myClass->startService($service, $wkey, $command);
		     #$result = 0;
		}
		return $result;
	}
        
        /**
	 * endWorkflow
	 * 
	 * @param string $wkey a key for the run
	 * @return string Response 
	*/
	public function endWorkflow($wkey){
		$myClass = new funcs();
		#Check if the workflow ended 
		$result = $myClass->endWorkflow($wkey); 
                
		return $result;
	}
	
	/**
        * Insert a job to database
        * 
        * @param string $username user of the run
        * @param string $wkey a key for the run
        * @param string $com and parameters for the service separated with |
        * @param string $jobname jobname that is going to be run
        * @param string $servicename service name that is going to be run
        * @param string $jobnum service name that is going to be run
        * @param string $result result
        * @return string Response
        */
        public function insertJob($username, $wkey , $com , $jobname, $servicename , $jobnum, $result){
               #return "insertJob:username=$username, wkey=$wkey, com=$com, jobname=$jobname, ser=$servicename, jobnum=$jobnum, res=$result";

               $myClass = new funcs();
               $res = $myClass->insertJob($username, $wkey , $com , $jobname, $servicename , $jobnum, $result);
               if ($res!="True")
               {
                    return "ERROR 150: There is an error to insert for job=$jobname, jobnumber=$jobnum"; 
               }

               return $res;
        }

        /**
         * update a job in database
         * 
         * @param string $username user of the run
         * @param string $wkey a key for the run
         * @param string $jobname jobname that is going to be run
         * @param string $servicename service name that is going to be run
         * @param string $field update field: submit, start or end 
         * @param string $jobnum service name that is going to be run
         * @param string $result result
         * @return string Response
         */
         public function updateJob($username, $wkey , $jobname, $servicename, $field, $jobnum, $result){
               #return "updateJob:username=$username, wkey=$wkey, jobname=$jobname, ser=$servicename, field=$field, jobnum=$jobnum, res=$result";
               $myClass = new funcs();
               $res = $myClass->updateJob($username, $wkey , $jobname, $servicename, $field, $jobnum, $result);
               return $res;
               if ($res!="True")
               {
                    return "ERROR 151: There is an error to update $field for $jobnum"; 
               }

               return $res;
         }

        /**
         *  Check if all the jobs are finished for a service
         * 
         * @param string $username user of the run
         * @param string $wkey a key for the run
         * @param string $servicename service name that is going to be run
         * @return string Response
         */
         public function checkAllJobsFinished($username, $wkey, $servicename){
                #return "checkAllJobsFinished:USERNAME=$username, WKEY=$wkey, SERVICE=$servicename";
                $myClass = new funcs();
                $res = $myClass->checkAllJobsFinished($username, $wkey , $servicename);
                #if ($res!="True")
                #{
                #    return "ERROR 152: There is an error in the service $servicename!"; 
                #}

                return $res;
         }
        /**
         * Insert a job output to db
         * 
         * @param string $username user of the run
         * @param string $wkey a key for the run
         * @param string $jobnum job number
         * @param string $jobout job output
         * @return string Response
         */
         function insertJobOut($username, $wkey , $jobnum, $jobout)
         {
                $myClass = new funcs();
                $res = $myClass->insertJobOut($username, $wkey , $jobnum, $jobout);
                if ($res!="True")
                {
                    return "ERROR 153: There is an error in the service $servicename!"; 
                }

                return $res;
         }

}

