
/***********************************************************************************************/
/***********************************************************************************************/
/* 
THE STORE PROCEDURE Run_Snapshot_Order_No_SP_Full_Load  WILL COMPLETE THE FOLLOWING TASKS:
1. CREATE A TEMPORARY TABLE #ORDER_NO_TO_BE_PROCESSED TO CREATE A LIST OF ORDER_NO TO BE PROCESSED FROM SNAPSHOT TABLE:
   eg CAST(SITE_LOCAL_DATETIME AS DATE)>'07/12/2019' ('07/12/2019' is the date which last incremental load was completed)
2. CALL Backlog_Amount_SP_Full_Load TO PROCESS EACH ORDER_NO FROM THE LIST, AND PRINT ERROR MESSAGES AND INSERT TO ERROR_LOG TABLE IF ANY.
3. The tables will be updated OR created during the run time : 
                                   ERROR_HANDLING TABLES:               ERROR_LOG, ERROR_HANDLING_BOOKING, 
								   PROCESSED_FLAG TABLES:               RAW_ORDER_LINE_SNAPSHOT, RAW_CUSTOMER_ORDER_HISTORY, RAW_CUSTOMER_ORDER_LINE_HIST
								   ORDER_NO WITH INCOMPLETE DATA:       INCOMPLETE_ORDER
								   FACT TABLE:                          IFSAPP_FACT_CUSTOMER_ORDER_LINE_BOOKINGS
								   INTERMEDIATE TABLE:                  IFSAPP_INTERMED_CUSTOMER_ORDER_LINE_BOOKINGS
NOTE:
   ERROR_LOG TABLE - CONTAIN THE ERROR INFORMATION FOR EACH ORDER_NO DURING THE RUN TIME.
   ERROR_HANDLING_BOOKING TABLE - CONTAIN THE MINIMUM COLUMNS FOR THE BOOKING CALCULATION OF A ORDER_NO WHICH HAS THE LOGIC BUG. 
*/
/***********************************************************************************************/
/***********************************************************************************************/

/* Beginning of Run_Snapshot_Order_No_SP_Full_Load */
CREATE PROCEDURE [dbo].[Run_Snapshot_Order_No_SP_Full_Load] 
AS     
BEGIN

DECLARE @COUNT NUMERIC(18), @PO NVARCHAR(12), @LAST NUMERIC(18)
SET @COUNT=1
SET @LAST=0
SET @PO=NULL

/* CREATE A TEMP TABLE #ORDER_NO_TO_BE_PROCESSED THAT CONTAINS ALL THE ORDER_NO NEED TO BE PROCESSED  */
	  IF OBJECT_ID('tempdb..#ORDER_NO_TO_BE_PROCESSED') IS NOT NULL                   
			begin
					drop table #ORDER_NO_TO_BE_PROCESSED
			end

      
	  CREATE TABLE #ORDER_NO_TO_BE_PROCESSED (ID numeric(18,0) IDENTITY(1,1), ORDER_NO NVARCHAR(12))
	  
	  /* ONLY SHOW THE PO WHICH HAS NOT BEEN PROCESSED BEFORE, NOT IN ERROR_HANDLING_BOOKING TABLE OR IN INCOMPLETE_ORDER TABLE*/
	   IF OBJECT_ID ('[dbo].[ERROR_HANDLING_BOOKING]') IS NOT NULL 
		  AND OBJECT_ID ('[dbo].[INCOMPLETE_ORDER]') IS NOT NULL
				 BEGIN
						   INSERT INTO #ORDER_NO_TO_BE_PROCESSED
						   SELECT PO_LIST.ORDER_NO
						   FROM
								 (SELECT DISTINCT ORDER_NO 
								  FROM 
										  ( 

										      
											   SELECT HISTORY_NO, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO
											   FROM [dbo].[RAW_ORDER_LINE_SNAPSHOT] SP                           									   										  

											   EXCEPT 

											   SELECT HISTORY_NO, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO
											   FROM [dbo].[ERROR_HANDLING_BOOKING]  EHB
									   
											   EXCEPT

											   SELECT HISTORY_NO, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO
											   FROM [dbo].[INCOMPLETE_ORDER]   MISSING
											   
										   )  NOT_UPDATED
						   
								   )AS PO_LIST

				 END

		ELSE IF  OBJECT_ID('[dbo].[ERROR_HANDLING_BOOKING]') IS NULL 
				 AND OBJECT_ID('[dbo].[INCOMPLETE_ORDER]') IS NOT NULL
				 BEGIN
				       INSERT INTO #ORDER_NO_TO_BE_PROCESSED
					   SELECT PO_LIST.ORDER_NO
					   FROM
							 (SELECT DISTINCT ORDER_NO 
							  FROM 
									  ( 
										   SELECT HISTORY_NO, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO
										   FROM [dbo].[RAW_ORDER_LINE_SNAPSHOT] SP
                           									       
										   EXCEPT

										   SELECT HISTORY_NO,ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO
										   FROM [dbo].[INCOMPLETE_ORDER] MISSING
													   
									   )  NOT_UPDATED
						   
							   )AS PO_LIST
				END
        
		ELSE IF  OBJECT_ID('[dbo].[ERROR_HANDLING_BOOKING]') IS NULL 
				 AND OBJECT_ID('[dbo].[INCOMPLETE_ORDER]') IS NULL
				 	
				BEGIN					
					   INSERT INTO #ORDER_NO_TO_BE_PROCESSED
					   SELECT PO_LIST.ORDER_NO
					   FROM
							 (SELECT DISTINCT ORDER_NO 
							  FROM  [dbo].[RAW_ORDER_LINE_SNAPSHOT] SP		 
							  ) AS PO_LIST
			       
				END

		
			 
				SELECT * FROM #ORDER_NO_TO_BE_PROCESSED
		
				SELECT @LAST=MAX(ID) FROM #ORDER_NO_TO_BE_PROCESSED 
				/* PRINT @LAST */

		

/* TREAT EACH ORDER_NO AS A TRANSACTION, IF ANY UNEXPECTED ERROR OCCURS, IT WILL STOP AND PRINT THE ORDER_NO AND ERROR MESSAGE. */
        WHILE @COUNT<=@LAST
					BEGIN
					      SELECT @PO=ORDER_NO
						  FROM #ORDER_NO_TO_BE_PROCESSED
						  WHERE ID=@COUNT
					      						  
						  BEGIN TRY						     

							  BEGIN TRANSACTION
							  EXEC Backlog_Amount_SP_Full_Load  @PO 
							  COMMIT TRANSACTION
							  
						  END TRY

						  BEGIN CATCH
							 IF @@TRANCOUNT > 0 
							 ROLLBACK
							 
							 PRINT 'THERE IS ERROR IN : ' + @PO + ',AND THE ERROR# IS '+ convert(NVARCHAR,ERROR_NUMBER()) +': '+ERROR_MESSAGE() 
							 PRINT ' OCCURED ON LINE# '+ CONVERT(NVARCHAR,ERROR_LINE())
					         
							 INSERT INTO [dbo].[ERROR_LOG]      --INSERT ErrorInfo INTO ERROR_LOG TABLE
							 SELECT									
									@PO AS ORDER_NO,																		
									ERROR_NUMBER() AS ErrorNumber,	
									ERROR_LINE() AS ErrorLine,								
									ERROR_MESSAGE() AS ErrorMessage,									
									getdate() AS ErrorDatetime
									
		                  END CATCH

						  SET @COUNT=@COUNT+1

					END 
		

END
/* END OF RUN_SNAPSHOT_ORDER_NO_SP_FULL_LOAD */
/***********************************************************************************/
/***********************************************************************************/
