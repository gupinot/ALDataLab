
function circos(data,nb_threshold,volume_threshold)
{
    if(typeof nb_threshold == 'undefined') nb_threshold = 100;
    if(typeof volume_threshold == 'undefined') volume_threshold = 1000000;
    $("#circos_result").show();
    $("#LoadingResult").show();
    $("#circosres").hide();


      /////////////////////////////////////////////////
      //circosGraph() R function return 2 links : 1ft for png circos result, 2nd for circos matric csv file
      var req = ocpu.call("circosGraph", {"input":data, "nb_threshold":nb_threshold, "volume_threshold":volume_threshold}, function(session){
      		//retrieve the returned object async
      		ocpusession=session.getKey();
      		session.getObject(function(data){
			  // display result
              		document.getElementById('circos_graph').src = data[0];
			$("#LoadingResult").hide();
			$("#circosres").show();
      		});
      	})
      	req.fail(function(){
   		alert("R returned an error: (session : " + ocpusession + ") " + req.responseText);
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
