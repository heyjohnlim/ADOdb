<?php
/*
@version   v5.21.0-dev  ??-???-2016
@copyright (c) 2000-2013 John Lim (jlim#natsoft.com). All rights reserved.
@copyright (c) 2014      Damien Regad, Mark Newnham and the ADOdb community
  Released under both BSD license and Lesser GPL library license.
  Whenever there is any discrepancy between the two licenses,
  the BSD license will take precedence. See License.txt.
  Set tabs to 4 for best viewing.
  Latest version is available at http://adodb.org/
 
 Google BigQuery driver for ADOdb.

 SETUP
 ====
 Install Composer (command line install)
  
  https://getcomposer.org/download/ 
 
 This will install a vendor directory in current directory
 
 Install google cloud libraries (this will be in vendor directory)
 See https://cloud.google.com/bigquery/docs/reference/libraries
 
 Install using this command (from current directory)
 
 php composer.phar require https://getcomposer.org/download/
 
 
 Sample Usage
 ============
 
 <?php
error_reporting(E_ALL);
require 'path/to/vendor/autoload.php';

include_once('/path/to/adodb/adodb.inc.php');
$DB = ADONewConnection('bigquery');

if (!$DB) die('$DB not defined\n');
echo "Connect...\n"; 
$ok = $DB->Connect('.\Project-1671ece42e1e.json','','','ProjectID');
if (!$ok) die();
 
$DB->debug=1;
$DB->setFetchMode(ADODB_FETCH_ASSOC);
$rs = $DB->Execute('SELECT * FROM `bigquery-public-data.chicago_crime.crime` LIMIT 10'); 
$arr = $rs->GetArray();
var_dump($arr); 

 */

use Google\Cloud\BigQuery\BigQueryClient;
 
// security - hide paths
if (!defined('ADODB_DIR')) die();

if (! defined("_ADODB_BIGTABLE_LAYER")) {
 define("_ADODB_BIGTABLE_LAYER", 1 );


class ADODB_bigquery extends ADOConnection {
	var $databaseType = 'bigquery';
	var $databaseProvider = 'bigquery';
	var $hasInsertID = false;
	var $hasAffectedRows = false;	
	var $fmtTimeStamp = "'Y-m-d H:i:s'";
	var $_affectedrows=0;
	var $_insertid=0;
	var $_url;
	var $replaceQuote = "''"; // string to use to replace quotes
	var $hasTransactions = false;
	var $_errorNo = false;
	var $_project;
    var $_bindInputArray = true;
    
    function __construct()
    {
    }
    
	function _insertid()
	{
			return $this->_insertid;
	}
	
	function _affectedrows()
	{
			return $this->_affectedrows;
	}
  
  	function MetaDatabases()
	{
		return false;
	}

	
	// returns true or false
    // pass in json credentials in $argHostname, $argDatabasename the project id
	function _connect($argHostname='', $argUsername='', $argPassword='', $argDatabasename='')
	{
        if ($argHostname) {
            putenv("GOOGLE_APPLICATION_CREDENTIALS=$argHostname"); // file that holds json credentials
        }
        /*if (!class_exists('BigQueryClient')) { // class_exists can fail because autoloader has not loaded class yet
            echo "BigQueryClient not installed";
            return false;
        }*/
        
        $this->_connectionID = new BigQueryClient([
            'projectId' => $argDatabasename,
        ]);
        $this->_project = $argDatabasename;
        
		return $this->_connectionID ? true : false;	
	}
	
	// returns true or false
	function _pconnect($argHostname, $argUsername, $argPassword, $argDatabasename)
	{
		return $this->_connect($argHostname, $argUsername, $argPassword, $argDatabasename);
	}
	
 	function MetaColumns($table, $normalize=true) 
	{
		return false;
	}
		
	
	// returns queryID or false
	function _Execute($sql,$inputarr=false)
	{
	global $ADODB_FETCH_MODE;
          
        try {
            if ($inputarr)   $queryJobConfig = $this->_connectionID->query($sql)->parameters($inputarr);
            else $queryJobConfig = $this->_connectionID->query($sql);
            $queryResults = $this->_connectionID->runQuery($queryJobConfig);
		
        } catch(Exception $e) {
            $this->_errorMsg = (string) $e->getMessage();
            echo $this->_errorMsg;
            $this->_errorNo = -99;
            if ($fn = $this->raiseErrorFn) {
				$fn($this->databaseType,'EXECUTE',$this->ErrorNo(),$this->ErrorMsg(),$sql,$inputarr);
			}
            return false;
        }
        
        // https://googleapis.github.io/google-cloud-php/#/docs/google-cloud/v0.135.0/bigquery/queryresults
		$rs = new ADORecordset_Array();
        //$info = $queryResults->info();
        
        $rows = $queryResults->rows();
        $cnt = 0;
        $names = array();
        $types = array();
        $data = array();
        if (!empty($this->fetchMode)) $mode = $this->fetchMode;
        else $mode = $ADODB_FETCH_MODE;
        
        if (empty($mode)) $mode = ADODB_FETCH_NUM;
        foreach($rows as $row) {
            $drow = array();
            $at = 0;
            foreach($row as $name=>$fldata) {

                $fld = $fldata;
                if (is_array($fld)) {
                    $fld = json_encode($fld);
                }
                elseif (is_object($fld)) {
                     $fld = (string) $fld;
                }
                        
                if ($cnt == 0) {
                    $names[] = strtoupper($name);
                    if (is_object($fldata)) {
                        $cls = get_class($fldata);
                        if (strpos($cls,'Timestamp')) $types[] = 'D';
                        elseif (strpos($cls,'Date')) $types[] = 'T';
                        else $types[] = 'C';
                    } else {
                        if (is_string($fld))
                            $types[] = 'C';
                        elseif(is_numeric($fld)) 
                            $types[] = 'N';
                        else 
                            $types[] = 'C';
                    }
                }
                
                if ($mode == ADODB_FETCH_ASSOC || $mode == ADODB_FETCH_BOTH) {
                    $drow[$names[$at]] = $fld;
                }   
                
                if ($mode == ADODB_FETCH_NUM || $mode == ADODB_FETCH_BOTH) {
                    $drow[] = $fld;
                }  
                $at += 1;
            }
            $data[] = $drow;
            $cnt += 1;
        }
        $rs->InitArray($data, $types, $names);
        
			
		if (is_object($rs)) {
			$rs->fetchMode = ($this->fetchMode !== false) ?  $this->fetchMode : $ADODB_FETCH_MODE;
			
			$this->_affectedrows = $rs->affectedrows;
			$this->_insertid = $rs->insertid;
			$rs->databaseType='bigquery';
			$rs->connection = $this;
		}
       
		return $rs;
	}
    
    function SelectLimit($sql,$nrows=-1,$offset=-1,$inputArr=false, $secs2cache = 0)
	{
		$nrows = (integer) $nrows;
		if ($offset <= 0) {
		// could also use " OPTIMIZE FOR $nrows ROWS "
			if ($nrows >= 0) 	$sql .=  " LIMIT $nrows";
			$rs = $this->Execute($sql,$inputArr);
		} else {
			if ($offset > 0 && $nrows < 0);
			else {
				$nrows += $offset;
				$sql .=  " LIMIT $nrows";
			}
			$rs = ADOConnection::SelectLimit($sql,-1,$offset,$inputArr);
		}
		
		return $rs;
	}
    

	/*	Returns: the last error message from previous database operation	*/	
	function ErrorMsg() 
	{
			return $this->_errorMsg;
	}
	
	/*	Returns: the last error number from previous database operation	*/	
	function ErrorNo() 
	{
		return $this->_errorNo;
	}
	
	// returns true or false
	function _close()
	{
		return true;
	}
} // class

class ADORecordset_bigtable extends ADORecordset {
    
    function __construct($id,$mode=false)
    {
        if ($mode === false) {  
			global $ADODB_FETCH_MODE;
			$mode = $ADODB_FETCH_MODE;
		}
		$this->fetchMode = $mode;
		
		$this->_queryID = $id;
    }
 

	function _close()
	{
		return true;
	}
}

} // define
	