var AppCSValreadyDownloaded=false;
var ServerCSValreadyDownloaded=false;
var ClusterArray;
var VolumeUnitStat="";

var temp=null;

var LstPannelSetting=null;
var datapath="/data/";
var AppDeviceFilterFileInclude="AppliDeviceFilterInclude.csv";
var AppDeviceFilterFileExclude="AppliDeviceFilterExclude.csv";
var AppFilterDeviceFile=AppDeviceFilterFileExclude;
var AppFilterDeviceFileUserUploaded=null;

var AppServerFilterFileInclude="AppliServerFilterInclude.csv";
var AppServerFilterFileExclude="AppliServerFilterExclude.csv";
var AppFilterServerFile=AppServerFilterFileExclude;
var AppFilterServerFileUserUploaded=null;



  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //	Functions to create list in pannel setting view
	function createSelectList(OptionValue, SelectDiv, Label)
    {
        var OptionFragment = document.createElement('option');
        OptionFragment.setAttribute('value', OptionValue);
        OptionFragment.innerHTML = Label;
        
        myselect = document.getElementById(SelectDiv);
        myselect.appendChild(OptionFragment);
    }

	function createRadioButton(buttonValue, buttonName, buttonDiv, checked)
	{
        var radioFragment = document.createElement('input');
        radioFragment.setAttribute('type', 'radio');
        radioFragment.setAttribute('name', buttonName);
        radioFragment.setAttribute('id', buttonName);
        radioFragment.setAttribute('value', buttonValue);
        if (checked == true) {
          radioFragment.setAttribute('checked', true);
        }
        
        mydiv = document.getElementById(buttonDiv);
        mydiv.appendChild(radioFragment);
        var radioLabel = document.createTextNode(buttonValue);
        //radioLabel.innerHTML = buttonValue;
        mydiv.appendChild(radioLabel);
        var br = document.createElement("br");
        mydiv.appendChild(br);
        
    }

	function createCheckBox(Value, Name, Div, checked, br, i)
    {
        var Fragment = document.createElement('input');
        Fragment.setAttribute('type', 'checkbox');
        Fragment.setAttribute('class', Name);
        Fragment.setAttribute('id', Name + i);
        Fragment.setAttribute('value', Value);
        if (checked == true) {
          Fragment.setAttribute('checked', true);
        }
        
        var Label = document.createElement('Label');
        Label.setAttribute("class","labelCheckBox");
        Label.setAttribute("for",Name+i);
        Label.innerHTML = Value;

        mydiv = document.getElementById(Div);
        mydiv.appendChild(Fragment);
        mydiv.appendChild(Label);
        if (br == true) {
	        var br = document.createElement("br");
    	    mydiv.appendChild(br);
    	}
      }    
  

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	function LstPannelSettingFunc() {
		var req = ocpu.call("LstPannelSetting", {}, function(session){

	      	//retrieve the returned object async
    	    session.getObject(function(data){
    	    	//data is the object returned by the R function
	    	    LstPannelSetting = data;
	    	    LstDateRange(LstPannelSetting[0]);
	    	    LstFromSite();
	    	    LstVolumeUnit();
	    	    LstSector();
	    	    LstCountry();
	    	    LstSite();
	    	    LstSiteCategory();
	    	    start_interaction();
				$("#WaitingMsg").hide();
	    	    
	    	});
    	})
	}

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // functions that fill pannel setting view from data returned by R functions
	function LstSite() {
		data=LstPannelSetting[5];
	    for (i=0; i<data.length; i++)
    	{
			if (data[i].SiteCode != "") {
				createSelectList(data[i].SiteCode, 'SiteSelectIn', data[i].SiteCode);    
	          	createSelectList(data[i].SiteCode, 'RemovedSiteSelectIn', data[i].SiteCode);
				createSelectList(data[i].SiteCode, 'SiteSelectOut', data[i].SiteCode);    
	          	createSelectList(data[i].SiteCode, 'RemovedSiteSelectOut', data[i].SiteCode);
		        }
    	}
        $("#SiteSelectIn").chosen({width: "100%"});
		$("#RemovedSiteSelectIn").chosen({width: "100%"});
    	$("#RemovedSiteSelectIn").trigger("chosen:updated");
        $("#SiteSelectOut").chosen({width: "100%"});
		$("#RemovedSiteSelectOut").chosen({width: "100%"});
    	$("#RemovedSiteSelectOut").trigger("chosen:updated");
	}
    
	function LstCountry() {
  		data=LstPannelSetting[4];
        
		for (i=1; i<data.length; i++)
    	{
					if (data[i] != "") {
          				createSelectList(data[i], 'CountrySelectIn', data[i]);
          				createSelectList(data[i], 'CountryExcludeSelectIn', data[i]);
          				createSelectList(data[i], 'CountrySelectOut', data[i]);
          				createSelectList(data[i], 'CountryExcludeSelectOut', data[i]);
	        		}
	    }
    	$("#CountrySelectIn").chosen({width: "100%"});
    	$("#CountryExcludeSelectIn").chosen({width: "100%"});
    	$("#CountrySelectOut").chosen({width: "100%"});
    	$("#CountryExcludeSelectOut").chosen({width: "100%"});
	}
      
	function LstSector() {
		data=LstPannelSetting[3];
   		//data is the object returned by the R function
    	for (i=1; i<data.length; i++)
		{
    	    createCheckBox(data[i], 'sectorselect', 'SectorFilter', true, true, i);
        }
	}

	function LstSiteCategory() {
		data=LstPannelSetting[6];
        for (i=0; i<data.length; i++)
		{
    		      createCheckBox(data[i], 'SiteCategoryselectIn', 'SiteCategoryFilterIn', true, true,i);
    		      createCheckBox(data[i], 'SiteCategoryselectOut', 'SiteCategoryFilterOut', true, true,i);
        }
	}
      
	function LstFromSite() {
		data=LstPannelSetting[1];
		for (i=0; i<data.length; i++)
        {
		    if (i==0) {
        		    checked = true;
		    }
        	else {
		            checked = false;
        	}
		    createSelectList(data[i], 'FromSiteMethodSelect', data[i]);
        }
	}
  
	function LstVolumeUnit() {
		data=LstPannelSetting[2];
		for (i=0; i<data.length; i++)
	    {
		    		if (i==0) {
        			    checked = true;
			        }
    			    else {
		        	    checked = false;
		    		}
			    	createSelectList(data[i].Id, 'VolumeUnitSelect', data[i].Label);
        }
	}
  
	function LstDateRange(data) {
    	   		temp=data;
	    	   	first_date=data.firstDate[0];
    		   	last_date=data.lastDate[0];
    	   	
    		   	$('input[name="daterange"]').daterangepicker(
    	   		{
					locale: {
				  		format: 'YYYY-MM-DD'
					},
					startDate: first_date,
					endDate: last_date,
					minDate: first_date,
					maxDate: last_date
				});
				$('input[name="daterange"]').prop('disabled', 'disabled');
	}


	function LstSubCluster(data) {

		//deselect all options of SubClusteringSelect menu
	  	var elements = document.getElementById("SubClusteringSelect").options;
    	for(var i = 0; i < elements.length; i++){
	      elements[i].selected = false;
	    }
	    
        var value = [];
        for (i=0; i<data.length; i++)
        {
          value[i]=data[i].Cluster; 
        }

		var unique=value.filter(function(itm,i,a){
   			 return i==a.indexOf(itm);
		});
        for (i=0; i<unique.length; i++)
        {
          createSelectList(unique[i], 'SubClusteringSelect', unique[i]);    
        }
        $("#SubClusteringSelect").chosen({width: "100%"});
        $("#SubClusteringSelect").trigger("chosen:updated");
	}
	

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // function to populate the pannel setting view and trigger change
  function populateNavigation() {
	LstPannelSettingFunc();       
  }
    
    
  ////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////////////////////
  function start_interaction() {
      
      
      
    $('#daterangeweekSelect').change(function(){
	    var valof = $(this).val();
	    switch(valof) {
	    	case "": LstDateRange(LstPannelSetting[0]);
	    		break;
	    	case "6Weeks": LstDateRange(LstPannelSetting[7]);
	    		break;
	    	case "LastWeek": LstDateRange(LstPannelSetting[8]);
	    		break;
	    	case "DayOne": LstDateRange(LstPannelSetting[9]);
	    		break;
	    }
    });

    $('#ClusteringActivated').change(function(){
    	if(this.checked) {
        	$('form[name="ClusteringSettingFormIn"]').show()
    	}
        else {
        	$('form[name="ClusteringSettingFormIn"]').hide()
        }
    });

    $('.DetailledFlowSelect').change(function(){
    	if(this.checked) {
        	$('tr[id="Server2ServerTr"]').show();
        	$('tr[id="PortFilteringTr"]').show();
    	}
        else {
        	$('tr[id="Server2ServerTr"]').hide();
        	$('.Server2ServerSelect').prop("checked", false);
        	$('tr[id="PortFilteringTr"]').hide();
        }
    });

    $('.sectorAll').change(function(){
      if(this.checked) {
            $('.SectorNA').attr("disabled", false);
            $('#SectorNADiv').show();
            $('.sectorselect').each(function (){
              this.checked=true;
            })
          }
          else {
            $('.sectorselect').each(function (){
              this.checked=false;
            })
            $('.SectorNA').prop("checked", false);
            $('#SectorNADiv').hide();
            $('.SectorNA').attr("disabled", true);
          }
    });

    $(".sectorselect").change(function() {
      if ($('.sectorselect:checked').length == $('.sectorselect').length) { //all checked
          $('.sectorAll').prop("checked", true);
          $('#SectorNADiv').show();
      }
      else
      {
        $('.sectorAll').prop("checked", false);
        $('#SectorNADiv').hide();
        $('.SectorNA').trigger('change').attr("checked", false);
      }
      
      if(!this.checked) {
        if ($('.sectorselect:checked').length == 0) // One box checked minimum
        {
            this.checked=true;
            $(this).trigger('change');
        }
      }
    });
    
    
    $('.SiteCategoryAllIn').change(function(){
      if(this.checked) {
            $('.SiteCategoryselectIn').each(function (){
              this.checked=true;
            })
          }
          else {
            $('.SiteCategoryselectIn').each(function (){
              this.checked=false;
            })
          }
    });
    
    $(".SiteCategoryselectIn").change(function() {
      if ($('.SiteCategoryselectIn:checked').length == $('.SiteCategoryselectIn').length) { //all checked
          $('.SiteCategoryAllIn').prop("checked", true);
      }
      else
      {
        $('.SiteCategoryAllIn').prop("checked", false);
      }
      
      if(!this.checked) {
        if ($('.SiteCategoryselectIn:checked').length == 0) // One box checked minimum
        {
              this.checked=true;
        }
      }
    });    

    $('.SiteCategoryAllOut').change(function(){
      if(this.checked) {
            $('.SiteCategoryselectOut').each(function (){
              this.checked=true;
            })
          }
          else {
            $('.SiteCategoryselectOut').each(function (){
              this.checked=false;
            })
          }
    });
    
    $(".SiteCategoryselectOut").change(function() {
      if ($('.SiteCategoryselectOut:checked').length == $('.SiteCategoryselectOut').length) { //all checked
          $('.SiteCategoryAllOut').prop("checked", true);
      }
      else
      {
        $('.SiteCategoryAllOut').prop("checked", false);
      }
      
      if(!this.checked) {
        if ($('.SiteCategoryselectOut:checked').length == 0) // One box checked minimum
        {
              this.checked=true;
        }
      }
    });    
    
	
	//////////////////////////////////////////////////////////////////////
	/// Appli filters rules
	//////////////////////////////////////////////////////////////////////    
    $("#AppFilterDeviceTypeSelect").change(function() {
    	if ($(this).val() == "none") {
    		$("#AppFilterDeviceFileSelect").hide();
    		$("#AppliFilterDeviceFile").hide();
    		$("#divuserdevicefile").hide();
    	}
    	else {
    		$("#AppFilterDeviceFileSelect").trigger('change').show();
		}
    });


    $("#AppFilterDeviceFileSelect").change(function() {
    	if ($(this).val() == "app") {
    		$("#divuserdevicefile").hide();
    		if ($("#AppFilterDeviceTypeSelect").val() == "include") {
	    		$("#AppliFilterDeviceFile").attr('href', datapath + AppDeviceFilterFileInclude);
	    		AppFilterDeviceFile=AppDeviceFilterFileInclude;

	    	}
	    	else
	    	{
	    		$("#AppliFilterDeviceFile").attr('href', datapath + AppDeviceFilterFileExclude);
	    		AppFilterDeviceFile=AppDeviceFilterFileExclude;
	    	}
	    	$("#AppliFilterDeviceFile").show();
    	}
    	else
    	{
    			//Afficher le champ input file userdevicefile
    			AppFilterDeviceFile=AppFilterDeviceFileUserUploaded;
	    		$("#AppliFilterDeviceFile").hide();
    			$("#divuserdevicefile").show();
    	}
    });

    $("#AppFilterServerTypeSelect").change(function() {
    	if ($(this).val() == "none") {
    		$("#AppFilterServerFileSelect").hide();
    		$("#AppliFilterServerFile").hide();
    		$("#divuserserverfile").hide();
    	}
    	else {
    		$("#AppFilterServerFileSelect").trigger('change').show();
		}
    });


    $("#AppFilterServerFileSelect").change(function() {
    	if ($(this).val() == "app") {
    		$("#divuserserverfile").hide();
    		if ($("#AppFilterServerTypeSelect").val() == "include") {
	    		$("#AppliFilterServerFile").attr('href', datapath + AppServerFilterFileInclude);
	    		AppFilterServerFile=AppServerFilterFileInclude;

	    	}
	    	else
	    	{
	    		$("#AppliFilterServerFile").attr('href', datapath + AppServerFilterFileExclude);
	    		AppFilterServerFile=AppServerFilterFileExclude;
	    	}
	    	$("#AppliFilterServerFile").show();
    	}
    	else
    	{
    			//Afficher le champ input file userserverfile
    			AppFilterServerFile=AppFilterServerFileUserUploaded;
	    		$("#AppliFilterServerFile").hide();
    			$("#divuserserverfile").show();
    	}
    });
    
    $("#MaxMatrixRange").change(function(){
  			var valof = $(this).val();
  			$('outputMaxMatrixRange').text(valof);
  	});
      

    $("#InflationParameter").change(function(){
  			var valof = $(this).val();
  			$('outputInflationParameter').text(valof);
  	});
      

    $("#ExpansionParameter").change(function(){
  			var valof = $(this).val();
  			$('outputExpansionParameter').text(valof);
  	});

    $("#ClusterNumber").change(function(){
  			var valof = $(this).val();
  			$('outputClusterNumber').text(valof);
  	});      
  	
    $("#ClusteringAlgoSelect").change(function(){
  			var valof = $(this).val();
  			switch(valof) { 
          case "mcl":
          	$("#MCLClusteringSettingDiv").show();
          	$("#KMeansClusteringSettingDiv").hide();
            break;
          case "kmeans":
          	$("#MCLClusteringSettingDiv").hide();
          	$("#KMeansClusteringSettingDiv").show();
            break;
        }
  			//$('outputClusterNumber').text(valof);
  	});
  	
    $("#panneauAppStat").change(function(){
  		if ($("#panneauAppStat").is(':visible')) 
		{
			if (!AppCSValreadyDownloaded) {
				AppliStat(document.getElementById('statcsv').href);
				AppCSValreadyDownloaded=true;
			}
		} 
  	});

    $("#panneauServerStat").change(function(){
  		if ($("#panneauServerStat").is(':visible')) 
		{
			if (!ServerCSValreadyDownloaded) {
				ServerStat(document.getElementById('servercsv').href);
				ServerCSValreadyDownloaded=true;
			}
		} 
  	});

    /*$('input[name="datacollectionsourcefilter"]').change(function(){
  		var valof = $(this).val();
  		switch(valof) { 
          case "Device":
          	LstDateRange(false, "Device");
            break;
          case "Server":
          	LstDateRange(false, "Server");
            break;
        }
  	});*/
    
    ///////////////////////////////////////////////////////////
    $("#userdevicefile").change(function(e) {
	
		if ($("#userdevicefile").val() != undefined && $("#userdevicefile").val() != "") {
			var ext = $("input#userdevicefile").val().split(".").pop().toLowerCase();
			if($.inArray(ext, ["csv"]) == -1) {
				alert('Upload CSV');
				AppFilterDeviceFileUserUploaded=null;
				return false;
			}
		}
		else { // no file selected
			switch($("#AppFilterDeviceTypeSelect").val()) {
				case "include":
					AppFilterDeviceFile=AppDeviceFilterFileInclude;
					break;
				case "exclude":
					AppFilterDeviceFile=AppDeviceFilterFileExclude;
					break;
			}
			AppFilterDeviceFileUserUploaded=null;
	  		$("#AppFilterDeviceFileSelect option[value='app']").attr('selected', 'selected');
	  		$("#AppFilterDeviceFileSelect").trigger('change');
			return true;
		}
		
		myfile=e.target.files[0];

		//perform the request
		var req = ocpu.call("UploadAppFilterFile", {
			"file" : myfile,
			}, function(session){
				session.getObject(function(outtxt){
					  AppFilterDeviceFile = outtxt[0];
					  AppFilterDeviceFileUserUploaded=AppFilterDeviceFile;
				});
		});
		
		//if R returns an error, alert the error message
		req.fail(function(){
			alert("Error downloading file");
			switch($("#AppFilterDeviceTypeSelect").val()) {
				case "include":
					AppFilterDeviceFile=AppDeviceFilterFileInclude;
					break;
				case "exclude":
					AppFilterDeviceFile=AppDeviceFilterFileExclude;
					break;
			}
			AppFilterDeviceFileUserUploaded=null;
	  		$("#AppFilterDeviceFileSelect option[value='app']").attr('selected', 'selected');
	  		$("#AppFilterDeviceFileSelect").trigger('change');
			return false
		});
		return true;
	});

    ///////////////////////////////////////////////////////////
    $("#userserverfile").change(function(e) {
	
		if ($("#userserverfile").val() != undefined && $("#userserverfile").val() != "") {
			var ext = $("input#userserverfile").val().split(".").pop().toLowerCase();
			if($.inArray(ext, ["csv"]) == -1) {
				alert('Upload CSV');
				AppFilterServerFileUserUploaded=null;
				return false;
			}
		}
		else { // no file selected
			switch($("#AppFilterServerTypeSelect").val()) {
				case "include":
					AppFilterServerFile=AppServerFilterFileInclude;
					break;
				case "exclude":
					AppFilterServerFile=AppServerFilterFileExclude;
					break;
			}
			AppFilterServerFileUserUploaded=null;
	  		$("#AppFilterServerFileSelect option[value='app']").attr('selected', 'selected');
	  		$("#AppFilterServerFileSelect").trigger('change');
			return true;
		}
		
		myfile=e.target.files[0];

		//perform the request
		var req = ocpu.call("UploadAppFilterFile", {
			"file" : myfile,
			}, function(session){
				session.getObject(function(outtxt){
					  AppFilterServerFile = outtxt[0];
					  AppFilterServerFileUserUploaded=AppFilterServerFile;
				});
		});
		
		//if R returns an error, alert the error message
		req.fail(function(){
			alert("Error downloading file");
			switch($("#AppFilterServerTypeSelect").val()) {
				case "include":
					AppFilterServerFile=AppServerFilterFileInclude;
					break;
				case "exclude":
					AppFilterServerFile=AppServerFilterFileExclude;
					break;
			}
			AppFilterServerFileUserUploaded=null;
	  		$("#AppFilterServerFileSelect option[value='app']").attr('selected', 'selected');
	  		$("#AppFilterServerFileSelect").trigger('change');
			return false
		});
		return true;
	});

    
    SetPanelParameters(GetUrlParameters());
    
    $('form').change(function() {
    	SetUrlParameters(GetPanelParameters());
    });
    
    $('input[name="daterange"]').prop('disabled', 'disabled');
    $('#navigation').show("slow");
    
    //circosGenerate();
    $("#SendButton").attr('onclick',"circosGenerate()");
  }
  

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	function DeviceView() {
		//show Device site resolution method, Detailled flow, 
	}

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	function ServerView() {
		
	}
  
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Function to generate result
    function circosGenerate() {
    
      $('html, body').animate({scrollTop:0}, 'slow');

      $("#SendButton").removeAttr('onclick');
      $("#LoadingResult").show();
      $("#circos_result").show();
      $("#circosres").hide();
      //$("#circos_graph").attr('src', 'img/circos_loading.png');
      $("#cluster").hide();
      $("#Appli").hide();
      $("#panneauAppStat").hide();
      $("#Server").hide();
      $("#panneauServerStat").hide();
      $("#navigation").hide("slow");
      
      /////////////////////////////////////////////////
      // get setting parameters
      var SectorSelect = new Array;
        SectorSelect = [];
        if (!$('.sectorAll').is(':checked') & $('.sectorselect:checked').length > 0) {
          $('.sectorselect:checked').each(function() {
            SectorSelect.push($(this).val());
          });  
        } else {SectorSelect=[""]}

	  var SiteCategorySelectIn = new Array;
        SiteCategorySelectIn = [];
        if (!$('.SiteCategoryAllIn').is(':checked') & $('.SiteCategoryselectIn:checked').length > 0) {
	          $('.SiteCategoryselectIn:checked').each(function() {
            SiteCategorySelectIn.push($(this).val());
          });  
        } else {SiteCategorySelectIn=[""]}

	  var SiteCategorySelectOut = new Array;
        SiteCategorySelectOut = [];
        if (!$('.SiteCategoryAllOut').is(':checked') & $('.SiteCategoryselectOut:checked').length > 0) {
	          $('.SiteCategoryselectOut:checked').each(function() {
            SiteCategorySelectOut.push($(this).val());
          });  
        } else {SiteCategorySelectOut=[""]}

      var SectorNA = Boolean($('.SectorNA:checked').val());
      
      var SiteSelectIn = $('#SiteSelectIn').val();
      if (SiteSelectIn == null) {
          SiteSelectIn=[""];
        }
      var SiteSelectOut = $('#SiteSelectOut').val();
      if (SiteSelectOut == null) {
          SiteSelectOut=[""];
        }
      
      var RemovedSiteSelectIn = $('#RemovedSiteSelectIn').val();
      if (RemovedSiteSelectIn == null) {
          RemovedSiteSelectIn=[""];
        }
      var RemovedSiteSelectOut = $('#RemovedSiteSelectOut').val();
      if (RemovedSiteSelectOut == null) {
          RemovedSiteSelectOut=[""];
        }

      var CountrySelectIn = $('#CountrySelectIn').val();
      if (CountrySelectIn == null) {
          CountrySelectIn=[""];
        }
      var CountrySelectOut = $('#CountrySelectOut').val();
      if (CountrySelectOut == null) {
          CountrySelectOut=[""];
        }

      var CountryExcludeSelectIn = $('#CountryExcludeSelectIn').val();
      if (CountryExcludeSelectIn == null) {
          CountryExcludeSelectIn=[""];
        }

      var CountryExcludeSelectOut = $('#CountryExcludeSelectOut').val();
      if (CountryExcludeSelectOut == null) {
          CountryExcludeSelectOut=[""];
        }
        
      var CountrySelectOperand = $('input[name="CountrySelectOperand"]:checked').val();
      var SiteSelectOperand = $('input[name="SiteSelectOperand"]:checked').val();
      var SiteCategorySelectOperand = $('input[name="SiteCategorySelectOperand"]:checked').val();
      var RemovedSiteSelectOperand = $('input[name="RemovedSiteSelectOperand"]:checked').val();
      var CountryExcludeSelectOperand = $('input[name="CountryExcludeSelectOperand"]:checked').val();
      
      
      var MaxMatrix = $("#MaxMatrixRange").val();
      var VolumeUnit = $("#VolumeUnitSelect").val();
      var FromSiteMethod = $("#FromSiteMethodSelect").val();
      var InterIntraSelect = $('input[name="interintrafilter"]:checked').val();
        InterIntraSelectRes = new Array;
        switch(InterIntraSelect) {
          case "All":
            InterIntraSelectRes=[true, true];
            break;
          case "Inter":
            InterIntraSelectRes=[true, false];
            break;
          case "Intra":
            InterIntraSelectRes=[false, true];
            break;
        }
        	
        var AppliFilteringDeviceType = $("#AppFilterDeviceTypeSelect").val();
		var AppFilterDeviceFileType = $("#AppFilterDeviceFileSelect").val();
		var AppliFilteringServerType = $("#AppFilterServerTypeSelect").val();
		var AppFilterServerFileType = $("#AppFilterServerFileSelect").val();
		if (AppliFilteringDeviceType != "none" && AppFilterDeviceFileType == "server" && AppFilterDeviceFileUserUploaded == null) {
			alert("You must upload your application filter device file or choose the application file in select list");
			$("#SendButton").attr('onclick',"circosGenerate()");
            $("#LoadingResult").hide();
			return(false);
		}
		if (AppliFilteringServerType != "none" && AppFilterServerFileType == "server" && AppFilterServerFileUserUploaded == null) {
			alert("You must upload your application filter server file or choose the application file in select list");
			$("#SendButton").attr('onclick',"circosGenerate()");
            $("#LoadingResult").hide();
			return(false);
		}


        var Clustering = Boolean($("#ClusteringActivated:checked").val());
	        	
	        	var ClusteringAlgoParam=$("#ClusteringAlgoSelect").val();
		        var DirectedGraphClusteringParam = Boolean($("#DirectedGraphClustering:checked").val());

		        var InflationParam = $("#InflationParameter").val();
    		    var ExpansionParam = $("#ExpansionParameter").val();
        		var MCLClusteringParam=[InflationParam, ExpansionParam];
		        var KmeanClusterNumberParam = $("#ClusterNumber").val();

		var Server2Server = Boolean($('.Server2ServerSelect:checked').val());
		
		var DetailledFlow = Boolean($('.DetailledFlowSelect:checked').val());

		var DateRange = $("#daterangeweekSelect").val();
		
		var portsfiltering = $("#PortFilterServerSelect").val();
		var portsfilterIncludeOrExclude=$("#PortServerFilterIncSelect").val();
    	
    	var ocpusession=0;
      
      /////////////////////////////////////////////////
      // cal siteMap() R function to generate results
      var req = ocpu.call("siteMap", {"Sector":SectorSelect, "SectorNA":SectorNA, 
      		"MaxMatrix":MaxMatrix, "VolumeUnit":VolumeUnit, "InterIntraSite":InterIntraSelectRes, 
      		"FromSite":FromSiteMethod, 
      		"SiteSelectIn":SiteSelectIn, "SiteSelectOut":SiteSelectOut, "SiteSelectOperand":SiteSelectOperand, 
      		"CountrySiteFilterIn":CountrySelectIn, "CountrySiteFilterOut":CountrySelectOut, "CountrySelectOperand":CountrySelectOperand,
      		"CountryExcludeSelectIn":CountryExcludeSelectIn, "CountryExcludeSelectOut":CountryExcludeSelectOut, "CountryExcludeSelectOperand":CountryExcludeSelectOperand,
      		"RemovedSiteSelectIn":RemovedSiteSelectIn, "RemovedSiteSelectOut":RemovedSiteSelectOut, "RemovedSiteSelectOperand":RemovedSiteSelectOperand, 
      		"SitesSectorScenarioFilterIn":SiteCategorySelectIn, "SitesSectorScenarioFilterOut":SiteCategorySelectOut, "SiteCategorySelectOperand":SiteCategorySelectOperand, 
      		"Clustering": Clustering, "ClusteringAlgo": ClusteringAlgoParam, "DirectedGraphClustering": DirectedGraphClusteringParam, "mclClusterParam":MCLClusteringParam, "kmeanClusterParam":KmeanClusterNumberParam,
      		"AppliFilteringDeviceType":AppliFilteringDeviceType, "AppFilterDeviceFileType":AppFilterDeviceFileType, "AppFilterDeviceFile":AppFilterDeviceFile,
      		"AppliFilteringServerType":AppliFilteringServerType, "AppFilterServerFileType":AppFilterServerFileType, "AppFilterServerFile":AppFilterServerFile,
      		"DetailledFlow":DetailledFlow, "Server2Server":Server2Server, "portsfilterIncludeOrExclude":portsfilterIncludeOrExclude, "portsfiltering":portsfiltering,
      		"DateRange":DateRange}, function(session){
      		//retrieve the returned object async
      		ocpusession=session.getKey();
      		session.getObject(function(data){
      			// for debugging, get R data output/console
				$("#key").text(session.getKey());
				$("#location").text(session.getLoc());

				//retrieve session console (stdout) async
				session.getConsole(function(outtxt){
					$("#output").text(outtxt);
				})
			  
			  // display result
              document.getElementById('circos_graph').src = data[0];
              if (data.length > 3) {
              	document.getElementById('circosimage').innerHTML = "Circos image";
              	document.getElementById('circosimage').href = data[0];
              	document.getElementById('circoscsv').innerHTML = "Circos matrix";
              	document.getElementById('circoscsv').href = data[2];
              	$("#dowloadData").show();

              	AppCSValreadyDownloaded=false;
              	VolumeUnitStat = VolumeUnit;
              	document.getElementById('statcsv').innerHTML = "Download Csv file";
              	document.getElementById('statcsv').href = data[3];
              	//AppliStat(document.getElementById('statcsv').href);
              	//document.getElementById('AppStatFile').href = data[3];
              	$("#AppStatSize").text("(" + data[5] + ")");
              	$("#Appli").show();
              	if (Clustering != false) {
	              	$(".ClusterPivotTable").pivotUI(data[4], {
				        rows: ["Cluster"],
				        aggregatorName: "List Unique Values",
				        vals: ["Site"],
					    });
					    ClusterArray=data[4];
					    LstSubCluster(data[4]);
	              	$("#cluster").show();
	            }
              	ServerCSValreadyDownloaded=false;
              	if (Server2Server) {
	              	document.getElementById('servercsv').innerHTML = "Download Csv file";
    	          	document.getElementById('servercsv').href = data[6];
    	          	$("#ServerStatSize").text("(" + data[7] + ")");
    	          	$("#Server").show();
              	}
			  }
			  $("#LoadingResult").hide();
			  $("#circosres").show();
              $("#SendButton").attr('onclick',"circosGenerate()");
      		});
      	})
      	req.fail(function(){
   			 alert("R returned an error: (session : " + ocpusession + ") " + req.responseText);
              $("#SendButton").attr('onclick',"circosGenerate()");
              $("#LoadingResult").hide();
		});
         
    }


  ////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////////
  // functions called from html
  
  function HideShowSwitch(DivId) {
  	if ($('#'+DivId).is(':visible')) {
  		$('#'+DivId).hide("slow");
  		$('#'+DivId).trigger("change");
  	}
  	else {
  		$('#'+DivId).show("slow");
  		$('#'+DivId).trigger("change");
  	}
  }

  ////////////////////////////////////////////
  // fill Site filtering list with predefined strategic data center
  function fill_StrategicDataCenterIn() {
    var StrategicDataCenters = ["MSO1", "MSO2", "CCH1", "BAD1", "NWK1", "HTL1", "VLU1", "RUE1", "SIN9", "HRV1"]
    var RemovedSiteSelectIn = $('#RemovedSiteSelectIn').val();
    if (RemovedSiteSelectIn == null) {
		for (var Site_ in StrategicDataCenters) {
			var site =StrategicDataCenters[Site_];
	  		$("#RemovedSiteSelectIn option[value='" + site + "']").attr('selected', 'selected');
		}
		$("#RemovedSiteSelectIn").trigger("chosen:updated");
	}
	else {
		//create again the list to skirt chosen firefox bug
		data=LstPannelSetting[5];
		$("#RemovedSiteSelectIn").empty();
	    for (i=0; i<data.length; i++)
    	{
			if (data[i].SiteCode != "") {
	          	createSelectList(data[i].SiteCode, 'RemovedSiteSelectIn', data[i].SiteCode);
		    }
    	}
		$("#RemovedSiteSelectIn").chosen({width: "100%"});
    	$("#RemovedSiteSelectIn").trigger("chosen:updated");
	}
    $('form').trigger('change');
  }

  function fill_StrategicDataCenterOut() {
    var StrategicDataCenters = ["MSO1", "MSO2", "CCH1", "BAD1", "NWK1", "HTL1", "VLU1", "RUE1", "SIN9", "HRV1"]
    var RemovedSiteSelectOut = $('#RemovedSiteSelectOut').val();
    if (RemovedSiteSelectOut == null) {
		for (var Site_ in StrategicDataCenters) {
			var site =StrategicDataCenters[Site_];
	  		$("#RemovedSiteSelectOut option[value='" + site + "']").attr('selected', 'selected');
		}
		$("#RemovedSiteSelectOut").trigger("chosen:updated");
	}
	else {
		data=LstPannelSetting[5];
		$("#RemovedSiteSelectOut").empty();
	    for (i=0; i<data.length; i++)
    	{
			if (data[i].SiteCode != "") {
	          	createSelectList(data[i].SiteCode, 'RemovedSiteSelectOut', data[i].SiteCode);
		    }
    	}
		$("#RemovedSiteSelectOut").chosen({width: "100%"});
    	$("#RemovedSiteSelectOut").trigger("chosen:updated");
	}
    $('form').trigger('change');
  }

  ////////////////////////////////////////////
  // launch sub clustering
  function SubClustering() {
  	var subcluster = $("#SubClusteringSelect").val();
  	if (subcluster == null) {
  		alert ("Select one cluster at least.");
  		return false;
  	}
  	
  	//deselect all options of SiteSelectIn menu
  	var elements = document.getElementById("SiteSelectIn").options;
    for(var i = 0; i < elements.length; i++){
      elements[i].selected = false;
    }
  	//deselect all options of SiteSelectOut menu
  	var elements = document.getElementById("SiteSelectOut").options;
    for(var i = 0; i < elements.length; i++){
      elements[i].selected = false;
    }
  	
  	
  	for (subcluster_ in subcluster) {
  		for (cluster_ in ClusterArray) {
  			if (ClusterArray[cluster_].Cluster == subcluster[subcluster_]) {
  				//delete country suffix in site name
  				var Site = ClusterArray[cluster_].Site.replace(/-..$/,"")
  				$("#SiteSelectIn option[value='" + Site + "']").attr('selected', 'selected');
  				$("#SiteSelectOut option[value='" + Site + "']").attr('selected', 'selected');
  			}
  		}
  	}
  	$("#SiteSelectIn").trigger("chosen:updated");
  	$("#SiteSelectOut").trigger("chosen:updated");
  	$('input[name="SiteSelectOperand"][value="AND"]').prop('checked', true)
  	$("#SiteSelectIn").trigger("change");
  	$("#SiteSelectOut").trigger("change");
  	circosGenerate();
  }
  
  

  
  
  ////////////////////////////////////////////
  ////////////////////////////////////////////
  window.onload = function() {
    $('#navigation').hide("slow");
    populateNavigation();
  }
  
  ////////////////////////////////////////////
  //to prevent enter key
  $(function() {
    $("form").submit(function() { return false; });
  });
  

  ////////////////////////////////////////////
  ////////////////////////////////////////////
//function to load csv link into pivot table
function AppliStat(csvlink) {
  var csv_as_array = [];
  $("#LoadingAppliStat").show();
  $.ajax({
    url: csvlink,
    async: true,
    dataType: "text" ,  
	scriptCharset: "utf-8" , 
    contentType: "application/json; charset=utf-8",
    success: function (csvd) {
        csv_as_array = $.csv.toArrays(csvd);
    }, 
    complete: function () {
    	var input = csv_as_array;
    	$(".AppliPivotTable").pivotUI(input, {
			        rows: ["Source.App.Name"],
			        aggregatorName: "Sum",
			        vals: [VolumeUnitStat],
				    });
		$("#LoadingAppliStat").hide();
    }
  });
}
    ////////////////////////////////////////////
  ////////////////////////////////////////////
//function to load csv link into pivot table
function ServerStat(csvlink) {
  var csv_as_array = [];
  $("#LoadingServerStat").show();
  $.ajax({
    url: csvlink,
    async: true,
    dataType: "text" ,  
	scriptCharset: "utf-8" , 
    contentType: "application/json; charset=utf-8",
    success: function (csvd) {
        csv_as_array = $.csv.toArrays(csvd);
    }, 
    complete: function () {
    	var input = csv_as_array;
    	$(".ServerPivotTable").pivotUI(input, {
			        rows: ["Source.App.Name"],
			        aggregatorName: "Sum",
			        vals: ["Traffic"],
				    });
		$("#LoadingServerStat").hide();
    }
  });
}


  ////////////////////////////////////////////
  ////////////////////////////////////////////
  // functions to manage panel results (statistics and clustering)
$(document).ready(function()
{
	// 0/ Masquage des panneaux par défaut
	$panneaux = $('div.panneau').hide();
	
	// ---------------------------------------------------------------------------------------- //
	
	// 1/ Initialisation des liens d'accès aux panneaux
	
	$('h2.titre').each(function(i)
	{
		$this = $(this);
		ancre = $this.next($panneaux)[0].id;
		
		lien = $('<a>',
		{
			'href':				'#' + ancre,
			'aria-expanded':	'false',
			'aria-controls':	ancre
		});
		
		$this.wrapInner(lien);
	});
	
	// ---------------------------------------------------------------------------------------- //

    // 2/ Gestion de l'affichage des panneaux
	$('#Appli > h2.titre > a').click(function() 
	{
		if ($(this).attr('aria-expanded') == 'false') 
		{
			if (!AppCSValreadyDownloaded) {
				AppliStat(document.getElementById('statcsv').href);
				AppCSValreadyDownloaded=true;
			}
            $(this).attr('aria-expanded', true).parent().next($panneaux).show();
		} 
		else 
		{
            $(this).attr('aria-expanded', false).parent().next($panneaux).hide();
		}
		return false;
	}); 
    // 2/ Gestion de l'affichage des panneaux
	$('#cluster > h2.titre > a').click(function() 
	{
		if ($(this).attr('aria-expanded') == 'false') 
		{
            $(this).attr('aria-expanded', true).parent().next($panneaux).show();
		} 
		else 
		{
            $(this).attr('aria-expanded', false).parent().next($panneaux).hide();
		}
		return false;
	}); 

});


////////////////////////////////////////////
////////////////////////////////////////////
// Functions below are defined to save or 
// get settings to or from URL parameters
////////////////////////////////////////////
////////////////////////////////////////////

////////////////////////////////////////////
////////////////////////////////////////////
function GetPanelParameters() {

	  var DeviceOrServer = $('input[name="datacollectionsourcefilter"]:checked').val();
      var VolumeUnit = $("#VolumeUnitSelect").val();
      var FromSiteMethod = $("#FromSiteMethodSelect").val();
      var MaxMatrix = $("#MaxMatrixRange").val();

      var InterIntraSelect = $('input[name="interintrafilter"]:checked').val();
        InterIntraSelectRes = new Array;
        switch(InterIntraSelect) {
          case "All":
            InterIntraSelectRes=[true, true];
            break;
          case "Inter":
            InterIntraSelectRes=[true, false];
            break;
          case "Intra":
            InterIntraSelectRes=[false, true];
            break;
        }
        	

      var SectorSelect = [];
        if (!$('.sectorAll').is(':checked') && $('.sectorselect:checked').length > 0) {
          $('.sectorselect:checked').each(function() {
            SectorSelect.push($(this).val());
          });  
        } else {SectorSelect=[""]}

      var SectorNA = Boolean($('.SectorNA:checked').val());
      
      var RemovedSiteSelectIn = $('#RemovedSiteSelectIn').val();
      if (RemovedSiteSelectIn == null) {
          RemovedSiteSelectIn=[""];
        }
      var RemovedSiteSelectOut = $('#RemovedSiteSelectOut').val();
      if (RemovedSiteSelectOut == null) {
          RemovedSiteSelectOut=[""];
        }
      var RemovedSiteSelectOperand = $('input[name="RemovedSiteSelectOperand"]:checked').val();

      var CountrySelectIn = $('#CountrySelectIn').val();
      if (CountrySelectIn == null) {
          CountrySelectIn=[""];
        }
      var CountrySelectOut = $('#CountrySelectOut').val();
      if (CountrySelectOut == null) {
          CountrySelectOut=[""];
        }
      var CountrySelectOperand = $('input[name="CountrySelectOperand"]:checked').val();
        
      var CountryExcludeSelectIn = $('#CountryExcludeSelectIn').val();
      if (CountryExcludeSelectIn == null) {
          CountryExcludeSelectIn=[""];
        }
      var CountryExcludeSelectOut = $('#CountryExcludeSelectOut').val();
      if (CountryExcludeSelectOut == null) {
          CountryExcludeSelectOut=[""];
        }
      var CountryExcludeSelectOperand = $('input[name="CountryExcludeSelectOperand"]:checked').val();

      var SiteSelectIn = $('#SiteSelectIn').val();
      if (SiteSelectIn == null) {
          SiteSelectIn=[""];
        }
      var SiteSelectOut = $('#SiteSelectOut').val();
      if (SiteSelectOut == null) {
          SiteSelectOut=[""];
        }
      var SiteSelectOperand = $('input[name="SiteSelectOperand"]:checked').val();
      
	  var SiteCategorySelectIn = [];
        if (!$('.SiteCategoryAllIn').is(':checked') && $('.SiteCategoryselectIn:checked').length > 0) {
	          $('.SiteCategoryselectIn:checked').each(function() {
            SiteCategorySelectIn.push($(this).val());
          });  
        } else {SiteCategorySelectIn=[""]}
	  var SiteCategorySelectOut = [];
        if (!$('.SiteCategoryAllOut').is(':checked') && $('.SiteCategoryselectOut:checked').length > 0) {
	          $('.SiteCategoryselectOut:checked').each(function() {
            SiteCategorySelectOut.push($(this).val());
          });  
        } else {SiteCategorySelectOut=[""]}
      var SiteCategorySelectOperand = $('input[name="SiteCategorySelectOperand"]:checked').val();

      
      var AppliFilteringDeviceType = $("#AppFilterDeviceTypeSelect").val();
	  var AppFilterDeviceFileSelect = $("#AppFilterDeviceFileSelect").val();
      var AppliFilteringServerType = $("#AppFilterServerTypeSelect").val();
	  var AppFilterServerFileSelect = $("#AppFilterServerFileSelect").val();

	  var Clustering = Boolean($("#ClusteringActivated:checked").val());
	        	
	  var ClusteringAlgoParam=$("#ClusteringAlgoSelect").val();
	  var DirectedGraphClusteringParam = Boolean($("#DirectedGraphClustering:checked").val());

	  var InflationParam = $("#InflationParameter").val();
      var ExpansionParam = $("#ExpansionParameter").val();
      var MCLClusteringParam=[InflationParam, ExpansionParam];
	  var KmeanClusterNumberParam = $("#ClusterNumber").val();


	  var DetailledFlow = Boolean($('.DetailledFlowSelect:checked').val());
	  var Server2Server = Boolean($('.Server2ServerSelect:checked').val());
	  
	  var DateRangeWeek = $("#daterangeweekSelect").val();
	  
	  var portsfiltering = $("#PortFilterServerSelect").val();
	  var portsfilterIncEx=$("#PortServerFilterIncSelect").val();
	  

	  var Parameters = {DeviceOrServer:DeviceOrServer, VolumeUnit:VolumeUnit, FromSiteMethod:FromSiteMethod,  
		  MaxMatrix:MaxMatrix, InterIntraSelectRes:InterIntraSelectRes, SectorSelect:SectorSelect, SectorNA:SectorNA, 
		  RemovedSiteSelectIn:RemovedSiteSelectIn, RemovedSiteSelectOut:RemovedSiteSelectOut, RemovedSiteSelectOperand:RemovedSiteSelectOperand,
		  CountrySelectIn:CountrySelectIn, CountrySelectOut:CountrySelectOut, CountrySelectOperand:CountrySelectOperand,
		  CountryExcludeSelectIn:CountryExcludeSelectIn, CountryExcludeSelectOut:CountryExcludeSelectOut, CountryExcludeSelectOperand:CountryExcludeSelectOperand,
		  SiteSelectIn:SiteSelectIn, SiteSelectOperand:SiteSelectOperand, SiteSelectOut:SiteSelectOut,
		  SiteCategorySelectIn:SiteCategorySelectIn, SiteCategorySelectOut:SiteCategorySelectOut, SiteCategorySelectOperand:SiteCategorySelectOperand, 
		  AppliFilteringDeviceType:AppliFilteringDeviceType, AppFilterDeviceFileSelect:AppFilterDeviceFileSelect, 
		  AppliFilteringServerType:AppliFilteringServerType, AppFilterServerFileSelect:AppFilterServerFileSelect,
		  Clustering:Clustering, ClusteringAlgoParam:ClusteringAlgoParam, DirectedGraphClusteringParam:DirectedGraphClusteringParam,
		  MCLClusteringParam:MCLClusteringParam, KmeanClusterNumberParam:KmeanClusterNumberParam, 
		  DetailledFlow:DetailledFlow, Server2Server:Server2Server, portsfiltering:portsfiltering, portsfilterIncEx:portsfilterIncEx, 
		  DateRangeWeek:DateRangeWeek};

	  return Parameters;
}


////////////////////////////////////////////
////////////////////////////////////////////
function SetPanelParameters(Parameters) {

	  for (var key in Parameters) {
	  	switch(key) {
	  		case "DeviceOrServer":
	  			$("input[name='datacollectionsourcefilter'][value='" + Parameters.DeviceOrServer + "']").prop('checked', true);
	  			$("#datacollectionsourcefilter").trigger('change');
	  			break;
	  		case "VolumeUnit":
	  			$("#VolumeUnitSelect option[value='" + Parameters.VolumeUnit + "']").attr('selected', 'selected');
	  			break;
	  		case "FromSiteMethod":
	  			$("#FromSiteMethodSelect option[value='" + Parameters.FromSiteMethod + "']").attr('selected', 'selected');
	  			break;
	  		case "MaxMatrix":
	  			$('outputMaxMatrixRange').text(Parameters.MaxMatrix);
	  			$("#MaxMatrixRange").attr('value', Number(Parameters.MaxMatrix));
	  			break;
	  		case "InterIntraSelectRes":
	  				switch(Parameters.InterIntraSelectRes[0]) {
	  					case "true" :
	  						if (Parameters.InterIntraSelectRes[1] == "true")
		  						var InterIntraSelect = "All";
		  					else
		  						var InterIntraSelect = "Inter";
	  						break;
	  					case "false":
	  						var InterIntraSelect = "Intra";
	  						break;
	  				}
	  			$("input[name='interintrafilter'][value='" + InterIntraSelect + "']").prop('checked', true);
	  			break;
	  		case "SectorSelect":
	  			$('.sectorselect').each(function (){
	              this.checked = false;
    	        })
    	        if (Parameters.SectorSelect == "") {
    	        	//all selected
		  			$('.sectorselect').each(function (){
		              this.checked = true;
    		        })
    		        $('.sectorAll').prop("checked", true);
    		        $('#SectorNADiv').show();
    	        } 
    	        else {
    	        	$('.sectorAll').prop("checked", false);
					for (sector_ in Parameters.SectorSelect) {
						$(".sectorselect[value='" + Parameters.SectorSelect[sector_] + "']").prop('checked', true);
					}
					$('#SectorNADiv').hide();
    	        }
    	        
	  			break;
	  		case "SectorNA":
	  			if (Parameters.SectorNA == "true")
	  				$('.SectorNA').prop("checked", true);
	  			else
	  				$('.SectorNA').prop("checked", false);
	  			break;
	  		case "RemovedSiteSelectIn":
	  		  	//deselect all options of RemovedSiteSelectIn menu
			  	var elements = document.getElementById("RemovedSiteSelectIn").options;
			    for(var i = 0; i < elements.length; i++){
			      elements[i].selected = false;
    			}

				if (Parameters.RemovedSiteSelectIn != "")
			  		for (site_ in Parameters.RemovedSiteSelectIn) {
		  				$("#RemovedSiteSelectIn option[value='" + Parameters.RemovedSiteSelectIn[site_] + "']").attr('selected', 'selected');
	  			}
	  			$("#RemovedSiteSelectIn").trigger("chosen:updated");
	  			break;
	  		case "RemovedSiteSelectOut":
	  		  	//deselect all options of RemovedSiteSelectIn menu
			  	var elements = document.getElementById("RemovedSiteSelectOut").options;
			    for(var i = 0; i < elements.length; i++){
			      elements[i].selected = false;
    			}

				if (Parameters.RemovedSiteSelectOut != "")
			  		for (site_ in Parameters.RemovedSiteSelectOut) {
		  				$("#RemovedSiteSelectOut option[value='" + Parameters.RemovedSiteSelectOut[site_] + "']").attr('selected', 'selected');
	  			}
	  			$("#RemovedSiteSelectOut").trigger("chosen:updated");
	  			break;
	  		case "CountrySelectIn":
	  		  	//deselect all options of RemovedSiteSelectIn menu
			  	var elements = document.getElementById("CountrySelectIn").options;
			    for(var i = 0; i < elements.length; i++){
			      elements[i].selected = false;
    			}
	
				if (Parameters.CountrySelectIn != "")
			  		for (site_ in Parameters.CountrySelectIn) {
		  				$("#CountrySelectIn option[value='" + Parameters.CountrySelectIn[site_] + "']").attr('selected', 'selected');
	  				}
	  			$("#CountrySelectIn").trigger("chosen:updated");
	  			break;
	  		case "CountrySelectOut":
	  		  	//deselect all options of RemovedSiteSelectIn menu
			  	var elements = document.getElementById("CountrySelectOut").options;
			    for(var i = 0; i < elements.length; i++){
			      elements[i].selected = false;
    			}
	
				if (Parameters.CountrySelectOut != "")
			  		for (site_ in Parameters.CountrySelectOut) {
		  				$("#CountrySelectOut option[value='" + Parameters.CountrySelectOut[site_] + "']").attr('selected', 'selected');
	  				}
	  			$("#CountrySelectOut").trigger("chosen:updated");
	  			break;
	  		case "CountrySelectOperand":
	  			$("input[name='CountrySelectOperand'][value='" + Parameters.CountrySelectOperand + "']").prop('checked', true);
	  			break;
	  		case "CountryExcludeSelectIn":
	  		  	//deselect all options of RemovedSiteSelectIn menu
			  	var elements = document.getElementById("CountryExcludeSelectIn").options;
			    for(var i = 0; i < elements.length; i++){
			      elements[i].selected = false;
    			}
	
				if (Parameters.CountryExcludeSelectIn != "")
			  		for (site_ in Parameters.CountryExcludeSelectIn) {
		  				$("#CountryExcludeSelectIn option[value='" + Parameters.CountryExcludeSelectIn[site_] + "']").attr('selected', 'selected');
	  				}
	  			$("#CountryExcludeSelectIn").trigger("chosen:updated");
	  			break;
	  		case "CountryExcludeSelectOut":
	  		  	//deselect all options of RemovedSiteSelectIn menu
			  	var elements = document.getElementById("CountryExcludeSelectOut").options;
			    for(var i = 0; i < elements.length; i++){
			      elements[i].selected = false;
    			}
	
				if (Parameters.CountryExcludeSelectOut != "")
			  		for (site_ in Parameters.CountryExcludeSelectOut) {
		  				$("#CountryExcludeSelectOut option[value='" + Parameters.CountryExcludeSelectOut[site_] + "']").attr('selected', 'selected');
	  				}
	  			$("#CountryExcludeSelectOut").trigger("chosen:updated");
	  			break;
	  		case "CountryExcludeSelectOperand":
	  			$("input[name='CountryExcludeSelectOperand'][value='" + Parameters.CountryExcludeSelectOperand + "']").prop('checked', true);
	  			break;
	  		case "SiteSelectIn":
	  		  	//deselect all options of SiteSelectIn menu
			  	var elements = document.getElementById("SiteSelectIn").options;
			    for(var i = 0; i < elements.length; i++){
			      elements[i].selected = false;
    			}

				if (Parameters.SiteSelectIn != "")
			  		for (site_ in Parameters.SiteSelectIn) {
		  				$("#SiteSelectIn option[value='" + Parameters.SiteSelectIn[site_] + "']").attr('selected', 'selected');
	  			}
	  			$("#SiteSelectIn").trigger("chosen:updated");
	  			$("#SiteSelectIn").trigger("change");
	  			break;
	  		case "SiteSelectOut":
	  		  	//deselect all options of SiteSelectIn menu
			  	var elements = document.getElementById("SiteSelectOut").options;
			    for(var i = 0; i < elements.length; i++){
			      elements[i].selected = false;
    			}

				if (Parameters.SiteSelectOut != "")
			  		for (site_ in Parameters.SiteSelectOut) {
		  				$("#SiteSelectOut option[value='" + Parameters.SiteSelectOut[site_] + "']").attr('selected', 'selected');
	  			}
	  			$("#SiteSelectOut").trigger("chosen:updated");
	  			$("#SiteSelectOut").trigger("change");
	  			break;		
	  		case "SiteSelectOperand":
	  			$("input[name='SiteSelectOperand'][value='" + Parameters.SiteSelectOperand + "']").prop('checked', true);
	  			break;
	  		case "SiteCategorySelectIn":
	  			$('.SiteCategoryselectIn').each(function (){
	              this.checked = false;
    	        })
    	        if (Parameters.SiteCategorySelectIn == "") {
    	        	//means all selected
		  			$('.SiteCategoryselectIn').each(function (){
		              this.checked = true;
    		        })
    		        $('.SiteCategoryAllIn').prop("checked", true);
    	        } 
    	        else {
    	        	$('.SiteCategoryAllIn').prop("checked", false);
					for (siteCat_ in Parameters.SiteCategorySelectIn) {
						$(".SiteCategoryselectIn[value='" + Parameters.SiteCategorySelectIn[siteCat_] + "']").prop('checked', true);
					}
    	        }
	  			break;
	  		case "SiteCategorySelectOut":
	  			$('.SiteCategoryselectOut').each(function (){
	              this.checked = false;
    	        })
    	        if (Parameters.SiteCategorySelectOut == "") {
    	        	//means all selected
		  			$('.SiteCategoryselectOut').each(function (){
		              this.checked = true;
    		        })
    		        $('.SiteCategoryAllOut').prop("checked", true);
    	        } 
    	        else {
    	        	$('.SiteCategoryAllOut').prop("checked", false);
					for (siteCat_ in Parameters.SiteCategorySelectOut) {
						$(".SiteCategoryselectOut[value='" + Parameters.SiteCategorySelectOut[siteCat_] + "']").prop('checked', true);
					}
    	        }
	  			break;
	  		case "SiteCategorySelectOperand":
	  			$("input[name='SiteCategorySelectOperand'][value='" + Parameters.SiteCategorySelectOperand + "']").prop('checked', true);
	  			break;
	  		case "AppFilterDeviceFileSelect":
	  			$("#AppFilterDeviceFileSelect option[value='" + Parameters.AppFilterDeviceFileSelect + "']").attr('selected', 'selected');
	  			$("#AppFilterDeviceFileSelect").trigger('change');
	  			$("#AppFilterDeviceTypeSelect").trigger('change');
	  			break;
	  		case "AppliFilteringDeviceType":
	  			$("#AppFilterDeviceTypeSelect option[value='" + Parameters.AppliFilteringDeviceType + "']").attr('selected', 'selected');
	  			$("#AppFilterDeviceTypeSelect").trigger('change');
	  			break;
	  		case "AppFilterServerFileSelect":
	  			$("#AppFilterServerFileSelect option[value='" + Parameters.AppFilterServerFileSelect + "']").attr('selected', 'selected');
	  			$("#AppFilterServerFileSelect").trigger('change');
	  			$("#AppFilterServerTypeSelect").trigger('change');
	  			break;
	  		case "AppliFilteringServerType":
	  			$("#AppFilterServerTypeSelect option[value='" + Parameters.AppliFilteringServerType + "']").attr('selected', 'selected');
	  			$("#AppFilterServerTypeSelect").trigger('change');
	  			break;
	  		case "Clustering":
	  			if (Parameters.Clustering == "true")
	  				$('#ClusteringActivated').prop("checked", true);
	  			else
					$('#ClusteringActivated').prop("checked", false);
	  			$('#ClusteringActivated').trigger('change');
	  			break;
	  		case "ClusteringAlgoParam":
	  			$("#ClusteringAlgoSelect option[value='" + Parameters.ClusteringAlgoParam + "']").attr('selected', 'selected');
	  			$("#ClusteringAlgoSelect").trigger('change');
	  			break;
	  		case "DirectedGraphClusteringParam":
	  			if (Parameters.DirectedGraphClusteringParam == "true")
	  				$('#DirectedGraphClustering').prop("checked", true);
	  			else
					$('#DirectedGraphClustering').prop("checked", false);
	  			break;
	  		case "MCLClusteringParam":
	  			$('outputinflationparameter').text(Parameters.MCLClusteringParam[0]);
	  			$("#InflationParameter").attr('value', Number(Parameters.MCLClusteringParam[0]));
	  			$('outputexpansionparameter').text(Parameters.MCLClusteringParam[1]);
	  			$("#ExpansionParameter").attr('value', Number(Parameters.MCLClusteringParam[1]));

	  			break;
	  		case "KmeanClusterNumberParam":
	  			$('outputclusternumber').text(Parameters.KmeanClusterNumberParam);
	  			$("#ClusterNumber").attr('value', Number(Parameters.KmeanClusterNumberParam));
	  			break;
	  		case "DetailledFlow":
	  			if (Parameters.DetailledFlow == "true")
	  				$('.DetailledFlowSelect').prop("checked", true);
	  			else
	  				$('.DetailledFlowSelect').prop("checked", false);
	  			$('.DetailledFlowSelect').trigger('change');
	  			break;
	  		case "DateRangeWeek":
	  			$("#daterangeweekSelect option[value='" + Parameters.DateRangeWeek + "']").attr('selected', 'selected');
	  			$("#daterangeweekSelect").trigger('change');
	  			break;
	  		case "Server2Server":
	  			if (Parameters.Server2Server == "true")
	  				$('.Server2ServerSelect').prop("checked", true);
	  			else
	  				$('.Server2ServerSelect').prop("checked", false);
	  			break;
	  		case "portsfilterIncEx":
	  			$("#PortServerFilterIncSelect option[value='" + Parameters.portsfilterIncEx + "']").attr('selected', 'selected');
	  			$("#PortServerFilterIncSelect").trigger('change');
	  			break;
	  		case "portsfiltering":
	  			$("#PortFilterServerSelect").val(Parameters.portsfiltering);
	  			$("#PortFilterServerSelect").trigger('change');
	  			break;
	  	}
	  }   
}

////////////////////////////////////////////
////////////////////////////////////////////
function GetUrlParameters() {
	var qs = document.location.search;
    qs = qs.split('+').join(' ');
    var params = {},
        tokens,
        re = /[?&]?([^=]+)=([^&]*)/g;
    while (tokens = re.exec(qs)) {
        params[decodeURIComponent(tokens[1])] = decodeURIComponent(tokens[2]);
    }
    
    //convert specific fields into Array
    $.each(params, function(index, value) {
    	switch(index) {
    		case "InterIntraSelectRes":
    		case "CountrySelectIn":
    		case "CountryExcludeSelectIn":
    		case "RemovedSiteSelectIn":
    		case "SectorSelect":
    		case "SiteCategorySelectIn":
    		case "SiteSelectIn":
    		case "CountrySelectOut":
    		case "CountryExcludeSelectOut":
    		case "RemovedSiteSelectOut":
    		case "SiteCategorySelectOut":
    		case "SiteSelectOut":
    		case "MCLClusteringParam":
    			params[index] = params[index].split(',');
    			break;
    	}
    });

    return params;
}

////////////////////////////////////////////
////////////////////////////////////////////
function SetUrlParameters(Parameters) {
	var qs = "?";
	var operand="";
	$.each(Parameters, function(index, value) {
		if ($.isArray(value)) {
			qs = qs + operand + index + "=" + value.join(',');
		} 
		else {
			qs = qs + operand + index + "=" + value;
		}
		operand='&';
	});
	qs = document.location.pathname + qs;
	history.replaceState({}, "", qs);
}


