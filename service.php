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
         * 
	 * @return string Response is a new random key if it is a new run otherwise old key.
	 */
	public function startWorkflow($inputparam, $defaultparam, $username, $workflow, $wkey, $outdir ){
		$myClass = new funcs(); 
                
                $status = "exist";
                if($wkey=='' || $wkey=='start')
                {
 		   $wkey = $myClass->getKey();
                   $status = "new";
                }
		$ret=$myClass->startWorkflow($inputparam, $defaultparam, $username, $workflow, $wkey, $status, $outdir);
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
        
}

