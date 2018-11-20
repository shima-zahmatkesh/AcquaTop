package acqua.query.result;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import acqua.config.Config;

public class DataPloting {
	

	public static void generateDataForPlotingAlphaSelectivity( int maxline){
	
	String [] percentage ={ "10", "20", "25", "30" , "40" , "50" , "60" ,"70" ,"80" , "90" };
	//String [] percentage ={ "30", "60"  };
	
			try {
		
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPloting_Alpha_Selectivity_"+ maxline +".csv")));
				String writeLine = "percentage,db,policy,CJD,selectivity\n";
				bw.write(writeLine);
				
				for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
					
					for (int p=0 ; p < percentage.length ; p++){
				
						BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_"+ db +"_" + percentage[p] + ".csv")));
						String line ="" , firstLine = "";
						
						for( int k =0; k <= maxline; k++){
							if ( k == 0)
								firstLine = br.readLine(); 
							else
								line = br.readLine();
						}
						String [] fisrtLineSplit = firstLine.split(",| ");
						String [] lineSplit = line.split(",| ");

						for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
							
							if ( fisrtLineSplit[i].equals("timestampe") ||  fisrtLineSplit[i].equals("Oracle") ||  fisrtLineSplit[i].equals("WST"))
								continue;
							System.out.println("i = " + i + "p = " + p + " "+ fisrtLineSplit[i]);
							writeLine = percentage[p]+","+db+","+ fisrtLineSplit[i]+","+ lineSplit[i]+","+(100-Integer.valueOf(percentage[p]))+"\n";
							bw.write(writeLine);
						}
					}
				}
			
				bw.close();
	
	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
}

	public static void generateDataForPlotingAlphaBudget(int maxline){
	
	String [] budget = { "1", "2", "3" , "4" , "5" , "6" ,"7" };
	//String [] budget = { "3",  "5" };

			try {
		
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPloting_Alpha_Budget_"+ maxline +".csv")));
				String writeLine = "budget,db,policy,CJD\n";
				bw.write(writeLine);
				
				for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
					
					for (int b=0 ; b < budget.length ; b++){
				
						BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_"+ db +"_" + budget[b] + ".csv")));
						String line ="" , firstLine = "";
						
						for( int k =0; k <= maxline; k++){
							if ( k == 0)
								firstLine = br.readLine(); 
							else
								line = br.readLine();
						}
						String [] fisrtLineSplit = firstLine.split(",| ");
						String [] lineSplit = line.split(",| ");
						for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
							if ( fisrtLineSplit[i].equals("timestampe") ||  fisrtLineSplit[i].equals("Oracle") ||  fisrtLineSplit[i].equals("WST"))
								continue;
							writeLine = budget[b]+","+db+","+ fisrtLineSplit[i]+","+ lineSplit[i]+"\n";
							bw.write(writeLine);	
						}
					}
				}
			
				bw.close();
	
	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
}

	public static void generateDataForPlotingMultiRun(int maxline){

			try {
		
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPloting.csv")));
				String writeLine = "iteration,policy,CJD\n";
				bw.write(writeLine);
				writeLine = "";
				BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/test.csv")));
				String line ="" , firstLine = "";
						
				firstLine = br.readLine(); 
				String [] fisrtLineSplit = firstLine.split(",| ");
				
				for ( int k  =  0 ; k <= maxline ; k++){
					line = br.readLine();
					String [] lineSplit = line.split(",| ");
						
					System.out.println(fisrtLineSplit.length + " line length"+lineSplit.length);
					for ( int i = 1 ; i < lineSplit.length ; i++){
						writeLine = writeLine.concat(lineSplit[0]+","+ fisrtLineSplit[i]+","+ lineSplit[i]+"\n" ) ;
					}		
				}
				bw.write(writeLine);
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	}

	public static void generateDataForPlotingCombineAlgorithm( int maxline){
	
		//String [] distanceThershold = { "500","1000","50000" ,"62","125","250","750","12500","25000","93","187","375","625","875","77","109","156","218","85","101","140","171","202","234","312","437","562","687","812","937"};    //Filtering Distance From Threshold
		String [] distanceThershold = { "500","1000","50000" ,"62","125","250","750","12500","25000","93","187","375","625","875","77","109","156","218","85","101"};    //Filtering Distance From Threshold
		String [] percentage = {"30"};

		try {	
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPloting_CombineAlgorithm_"+ maxline +".csv")));
			String writeLine = "db,policy,distance,CJD,selectivity\n";
			bw.write(writeLine);
			for (int dt = 0 ; dt < distanceThershold.length ; dt++){	
				for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
					
					for (int p=0 ; p < percentage.length ; p++){
				
						BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ db +"_"+ distanceThershold[dt] + ".csv")));

						String line ="" , firstLine = "";
						
						for( int k =0; k <= maxline; k++){
							if ( k == 0)
								firstLine = br.readLine(); 
							else
								line = br.readLine();
						}
						String [] fisrtLineSplit = firstLine.split(",| ");
						String [] lineSplit = line.split(",| ");
						for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
							
							if ( fisrtLineSplit[i].equals("timestampe") ||  
								 fisrtLineSplit[i].equals("Oracle") ||  
								 fisrtLineSplit[i].equals("WST") ||
								 fisrtLineSplit[i].equals("RND") ||
								 fisrtLineSplit[i].equals("RND.F") )
								continue;
							writeLine = db +","+ fisrtLineSplit[i]+","+ distanceThershold[dt] +","+ lineSplit[i]+","+ (100-Integer.valueOf(percentage[p])) +"\n";
							bw.write(writeLine);
						}
					}
				}
			}
			bw.close();
		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		}		
}

	public static void generateDataForPlotingSelectivity( int maxline){
		
		String [] percentage ={ "10", "20", "25", "30" , "40" , "50" , "60" ,"70" ,"80" , "90" };
		
				try {
			
					BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPloting_Selectivity_"+ maxline +".csv")));
					String writeLine = "percentage,db,policy,CJD,selectivity\n";
					bw.write(writeLine);
					
					for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
						
						for (int p=0 ; p < percentage.length ; p++){
					
							BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ db +"_" + percentage[p] + ".csv")));
							String line ="" , firstLine = "";
							
							for( int k =0; k <= maxline; k++){
								if ( k == 0)
									firstLine = br.readLine(); 
								else
									line = br.readLine();
							}
							String [] fisrtLineSplit = firstLine.split(",| ");
							String [] lineSplit = line.split(",| ");
							for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
								
								if ( fisrtLineSplit[i].equals("timestampe") ||  fisrtLineSplit[i].equals("Oracle") ||  fisrtLineSplit[i].equals("WST"))
									continue;
							
								System.out.println(db+ "   i = " + i + "   p = " + p + " "+ fisrtLineSplit[i]);
								writeLine = percentage[p]+","+db+","+ fisrtLineSplit[i]+","+ lineSplit[i]+","+(100-Integer.valueOf(percentage[p]))+"\n";
								
								bw.write(writeLine);
							}
						}
					}
				
					bw.close();
		
		
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
	}
	
	public static void generateDataForPlotingBudget( int maxline){
		
		String [] budget ={ "1", "2",  "3" , "4" , "5" , "6" ,"7"  };
		
				try {
			
					BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPloting_Budget_"+ maxline +".csv")));
					String writeLine = "budget,db,policy,CJD\n";
					bw.write(writeLine);
					
					for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
						
						for (int b=0 ; b < budget.length ; b++){
					
							BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ db +"_" + budget[b] + ".csv")));
							String line ="" , firstLine = "";
							
							for( int k =0; k <= maxline; k++){
								if ( k == 0)
									firstLine = br.readLine(); 
								else
									line = br.readLine();
							}
							String [] fisrtLineSplit = firstLine.split(",| ");
							String [] lineSplit = line.split(",| ");
							for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
								
								if ( fisrtLineSplit[i].equals("timestampe") ||  fisrtLineSplit[i].equals("Oracle") ||  fisrtLineSplit[i].equals("WST"))
									continue;
							
								System.out.println(db+ "   i = " + i + "   b = " + b + " "+ fisrtLineSplit[i]);
								writeLine = budget[b]+","+db+","+ fisrtLineSplit[i]+","+ lineSplit[i]+"\n";
								
								bw.write(writeLine);
							}
						}
					}
				
					bw.close();
		
		
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
	}

	public static void generateDataForPlotingAllItreations1( int maxline){

	String [] percentage ={ "30"  };
	
			try {
		
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPloting_All_Itrations.csv")));
				String writeLine = "iteration,db,policy,CJD\n";
				bw.write(writeLine);
				
				for (int db=1 ; db <= Config.INSTANCE.getDatabaseNumber() ; db++){
					
					for (int p=0 ; p < percentage.length ; p++){
				
						BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_"+ db +"_" + percentage[p] + ".csv")));
						String line ="" , firstLine = "";
						
						for( int k =0; k <= maxline; k++){
							
							if ( k == 0){
								firstLine = br.readLine(); 
							}
							else{
								line = br.readLine();
						
								String [] fisrtLineSplit = firstLine.split(",| ");
								String [] lineSplit = line.split(",| ");
								for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
							
									if ( fisrtLineSplit[i].equals("timestampe") ||  fisrtLineSplit[i].equals("Oracle") ||  fisrtLineSplit[i].equals("WST"))
										continue;
									writeLine = k+","+ db +","+ fisrtLineSplit[i]+","+ lineSplit[i] +"\n";
							
									bw.write(writeLine);
								}
							}
						}
					}
				}
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
}

	public static void generateDataForPlotingAllItreations2( int maxline){

		String [] percentage ={ "30"  };
		
				try {
			
					BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPloting_All_Itrations2.csv")));
					String writeLine = "iteration,policy,CJD\n";
					bw.write(writeLine);
					
						for (int p=0 ; p < percentage.length ; p++){
					
							BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_"+ percentage[p] + ".csv")));
							String line ="" , firstLine = "";
							
							for( int k =0; k <= maxline; k++){
								
								if ( k == 0){
									firstLine = br.readLine(); 
								}
								else{
									
									line = br.readLine();
									String [] fisrtLineSplit = firstLine.split(",| ");
									String [] lineSplit = line.split(",| ");
									for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
								
										if ( fisrtLineSplit[i].equals("timestampe") ||  fisrtLineSplit[i].equals("Oracle") ||  fisrtLineSplit[i].equals("WST"))
											continue;
										System.out.println("i = " + i + "k = " + k + " "+ fisrtLineSplit[i]);
										writeLine = k+","+ fisrtLineSplit[i]+","+ lineSplit[i] +"\n";
								
										bw.write(writeLine);
									}
								}
							}
					}
					bw.close();
		
		
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
	}

	public static void generateDataForPlotingMTKN( String metric , String measureTopic ){
	
		String path = Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder( );
				try {
			
					BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path+"joinOutput/"+ metric +"CompareTotal_"+ measureTopic +".csv")));
					String writeLine = measureTopic + ",db,policy,NDCG\n";
					bw.write(writeLine);
					
					for (int db=1 ; db<=Config.INSTANCE.getDatabaseNumber() ; db++){
											
							BufferedReader br=new BufferedReader(new FileReader(new File(path+"joinOutput/"+ metric +"CompareTotal_"+ measureTopic +"_" + db + ".csv")));
							String line ="" , firstLine = "";
							
							firstLine = br.readLine();
							System.out.println ("metrics  "  + metric + "   measureTopic" + measureTopic + "   firstLine = "  + firstLine) ;
							String [] fisrtLineSplit = firstLine.split(",| ");
							while	((line = br.readLine())!= null ){
								
								System.out.println ("Line = "  + line) ;
								String [] lineSplit = line.split(",| ");
						
								for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
								
								if ( fisrtLineSplit[i].equals(measureTopic) || fisrtLineSplit[i].equals("timestampe") ||  fisrtLineSplit[i].equals("Oracle") ||  fisrtLineSplit[i].equals("OracleMTKN"))
									continue;
							
								writeLine = lineSplit[0]+","+db+","+ fisrtLineSplit[i]+","+ (150f-Float.parseFloat(lineSplit[i]))+"\n";
								
								bw.write(writeLine);
							}
						}
					}
				
					bw.close();
		
		
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
	}
	
	public static void generateDataForPlotingAllItreationsMTKN( String fileName){

		String [] percentage ={ "30"  };
		
				try {
			
					BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPlotingAllItration" + fileName)));
					String writeLine = "iteration,policy,NDCG\n";
					bw.write(writeLine);
					
						for (int p=0 ; p < percentage.length ; p++){
					
							BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/" + fileName)));
							String line ="" , firstLine = "";
							
							for( int k =0; k <= 150; k++){
								
								if ( k == 0){
									firstLine = br.readLine(); 
								}
								else{
									
									line = br.readLine();
									String [] fisrtLineSplit = firstLine.split(",| ");
									String [] lineSplit = line.split(",| ");
									for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
								
										if ( fisrtLineSplit[i].equals("timestampe") ||  fisrtLineSplit[i].equals("Oracle") ||  fisrtLineSplit[i].equals("OracleMTKN")  )
											continue;
										System.out.println("i = " + i + "k = " + k + " "+ fisrtLineSplit[i]);
										writeLine = k +","+ fisrtLineSplit[i]+","+ ( k - Float.parseFloat(lineSplit[i])) +"\n";
								
										bw.write(writeLine);
									}
								}
							}
					}
					bw.close();
		
		
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
	}

	
	public static void generateDataForPlotingFilter( String metric , String measureTopic , String type ){
		
		String path = Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder( );
				try {
			
					BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path+"joinOutput/"+ measureTopic + type + "Final.csv")));
					String writeLine = measureTopic + ",db,policy,CJD\n";
					bw.write(writeLine);
					
					for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
											
							BufferedReader br=new BufferedReader(new FileReader(new File(path+"joinOutput/"+ measureTopic + type + ".csv")));
							String line ="" , firstLine = "";
							
							firstLine = br.readLine(); 
							String [] fisrtLineSplit = firstLine.split(",| ");
							System.out.println ("firstLine = "  + firstLine) ;	
							while	((line = br.readLine())!= null ){
								
								System.out.println ("Line = "  + line) ;
								String [] lineSplit = line.split(",| ");
						
								for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
								
								if ( fisrtLineSplit[i].equals(measureTopic) || fisrtLineSplit[i].equals("db"))
									continue;
							
								writeLine = lineSplit[0]+","+ db +","+ fisrtLineSplit[i]+","+ Float.parseFloat(lineSplit[i])+"\n";
								
								bw.write(writeLine);
							}
						}
					}
				
					bw.close();
		
		
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
	}
	
	public static void main(String[] args){
	
//		generateDataForPlotingAlphaBudget(110);
//		generateDataForPlotingAlphaSelectivity(110);
//		generateDataForPlotingMultiRun(28);
//		generateDataForPlotingSelectivity(75);
//		generateDataForPlotingBudget(139);
//		generateDataForPlotingAllItreations1(140);
//	    generateDataForPlotingAllItreations2(140);
		 
		
//		 generateDataForPlotingMTKN("NDCG" ,"budget");
//		 generateDataForPlotingMTKN("NDCG" ,"K");
//		 generateDataForPlotingMTKN("NDCG" ,"N");
//		 generateDataForPlotingMTKN("NDCG" ,"CH");
		
//		 generateDataForPlotingMTKN("ACCK" ,"budget");
//		 generateDataForPlotingMTKN("ACCK" ,"K");
//		 generateDataForPlotingMTKN("ACCK" ,"N");
//	 	 generateDataForPlotingMTKN("ACCK" ,"CH");
		
// 		 150 should be removed from function
//		 generateDataForPlotingMTKN("precision" ,"budget");
//		 generateDataForPlotingMTKN("precision" ,"K");
//		 generateDataForPlotingMTKN("precision" ,"N");
//		 generateDataForPlotingMTKN("precision" ,"CH");
		 
//		 generateDataForPlotingMTKN("avgPrecisionAtK" ,"budget");
//		 generateDataForPlotingMTKN("avgPrecisionAtK" ,"K");
//		 generateDataForPlotingMTKN("avgPrecisionAtK" ,"N");
//		 generateDataForPlotingMTKN("avgPrecisionAtK" ,"CH");
		 
//		 generateDataForPlotingMTKN("accuracy" ,"budget");
//		 generateDataForPlotingMTKN("accuracy" ,"K");
//		 generateDataForPlotingMTKN("accuracy" ,"N");
//		 generateDataForPlotingMTKN("accuracy" ,"CH");
		 

		 generateDataForPlotingAllItreationsMTKN( "precisionCompare_budget_7.csv");
//	 	 generateDataForPlotingAllItreationsMTKN( "ACCKcompare-CH20-N10-B7-K5.csv");
		
//		 generateDataForPlotingFilter( "CJD" , "budget" , "Simple" );
//		 generateDataForPlotingFilter( "CJD" , "budget" , "Combine" );
//		 generateDataForPlotingFilter( "CJD" , "selectivity" , "Simple" );
//		 generateDataForPlotingFilter( "CJD" , "selectivity" , "Combine" );
	}

	
}
