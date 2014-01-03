<?php


include('funcs.php');
class funClass
{
  public function getFunc()
  {
    $myClass = new funcs();
    return  $myClass->getKey();
  }
  public function startService( $service, $wkey, $command){
      return $command;
                $myClass = new funcs();
                #Check if the job is started
                $result = $myClass->checkStatus($service, $wkey);
                if ( $result == "START" ) # Job hasn't started yet
                {
                     $result=$myClass->startService($service, $wkey, $command);
                     #$result = 0;
                }
                return $result;
  }
}

$myClass1 = new funClass();
#$myTrans = new funcs();
#$myTrans->writeTransaction("AAA", "BBB", $myClass1->getFunc(), "params");
#$myTrans->writeTransaction("AAA", "BBB", "CCCC", "params");

$myClass = new funcs();
print "AAA<BR>";
print $myClass->getKey()."<BR>";

$username="svcgalaxy";
$workflow="ChipSeqWorkflow";
$key="b3PIGiDj5xu95kuonZaBWSxk52B";
$service="service4";
#service1:cYUVd6uCIhnljAMpEoJPQorEjPYitD:sdadsf
#(a=INPUTPARAM , c=DEFAULTPARAM , b=USERNAME , e=wfname, d=WKEY , f=OUTDIR)
#$result=$myClass->checkStatus( $service, $key);
#$result=$myClass->startWorkflow("~/scratch/workflow/a.txt", $username, $workflow, $key, "~/scratch/out");
$result=$myClass1->startService($service, $key, "ls -l ;sleep 10");
print "SERVICE<BR>"; 
print "RESULT:<BR>[".$result."]<BR>";

/*
$result=$myClass->checkJob("kucukura", "splitFastQ", "M8yoTBcjHdjUeM9n8a5vRBCMOiB097");
if ($result==0)
{
	print "[success]<BR>";
}

$result=$myClass->checkJob("AAA", "BBB", "CCCC114");
print "[$result]<BR>";
if ($result==-1)
{
  print "Create the Job<BR>";
  print $myClass->writeTransaction("AAA", "BBB", "CCCC114", "params");
}
else
{
  print "Job hasn't finished yet";
}
*/
?>
