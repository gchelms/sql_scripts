
IF OBJECT_ID('dbo.spBlkRptAverageClaimFrequencyTriangleByIncurredMonth') IS NOT NULL
BEGIN
  DROP PROCEDURE dbo.spBlkRptAverageClaimFrequencyTriangleByIncurredMonth
END
GO


--Begin Transaction
--exec spBlkRptAverageClaimFrequencyTriangleByIncurredMonth 1234, 4
--Rollback transaction



CREATE PROCEDURE dbo.spBlkRptAverageClaimFrequencyTriangleByIncurredMonth
    @QryQueryId INT
  , @UserId INT
AS
BEGIN
	SET NOCOUNT ON

	DECLARE @Statement NVARCHAR(MAX)
	DECLARE @JoinParam NVARCHAR(4000)
	DECLARE @From varchar(MAX)
	DECLARE @Where varchar(MAX)
	DECLARE @OrderBy varchar(max)

    SELECT @JoinParam = null

-- get enrollment aggregates from BlkReportsummary	
	Select a.*, MonthsRemaining = datediff(month, ReportDate, PolicyEndDate)
	into #Enrollmentbase
	From   
	   (
	   Select PolicyId
	   , convert(datetime, (convert(varchar,DATEPART(month,EffectiveDate))+ '-' +convert(varchar,DATEPART(year,EffectiveDate))) + '-1') as EffectiveDate
	   , convert(datetime,(Convert(varchar, ReportMonth) + '-' + Convert(varchar, ReportYear)) + '-1') as ReportDate
	   , (Enrollment1 + Enrollment2 + Enrollment3 + Enrollment4) as Enrollment
	   , MonthsInContract
	   , PolicyEndDate = dateadd(m, MonthsInContract, EffectiveDate) - 1
	   From BlkReportSummary blk
	   join PlcPolicy plc on plc.PlcPolicyId = blk.PolicyId
	   where ReportMonth <> 0 and ReportYear <> 0
	   ) a
	order by PolicyId, ReportDate



	Select e.PolicyId, EffectiveDate, ReportDate, Enrollment, MonthsInContract, PolicyEndDate, MonthsRemaining, MinMonths, 
	ExtraEnrollment = Case when MinMonths <= 0 or minmonths <> MonthsRemaining then 0
												When MinMonths > 0 and minMonths = MonthsRemaining then (Enrollment * MinMonths) 
											else 99
								  End
	, TotalEstimatedEnrollment = null
	-- Select *
	into #Enrollment
	From 
	#enrollmentbase e
	join 
	(
	Select a.PolicyId, MinMonths
	From (
	Select PolicyId, min(MonthsRemaining) MinMonths
	from #Enrollmentbase
	group by PolicyId
	)a
	)mins on mins.PolicyId = e.PolicyId


	update #Enrollment
	set TotalEstimatedEnrollment = Enrollment + ExtraEnrollment

	--Select * from #Enrollment

	Select EffectiveDate, Sum(TE) as TotalEnrollmentByEffectiveDate
	into #ClaimEnrollment
	From
	(
	Select PolicyId, EffectiveDate, Sum(TotalEstimatedEnrollment) as TE
	From #Enrollment
	group by PolicyId, EffectiveDate) sums
	group by EffectiveDate
	;

-- Create ClaimCount
	Create table #ClaimCount
	(PolicyId int,
	ParticipantId int,
	ClmPaymentRequestId int not null,
	ClmSubmissionId int not null,
	ClaimIncurredDate datetime not null,
	CIPart int not null,
	CheckDate datetime not null,
	CheckPart int not null,
	ClaimCount int null,
	RowNumber int not null,
	Primary Key Clustered (CIPart, RowNumber)
	)

	EXEC dbo.[spBlkQryBuildMDSLAnalysisSQL] @QryQueryId, @UserId, @From OUTPUT, @Where OUTPUT

	SELECT @Statement = 
	'Declare @QryQueryId int set @QryQueryId =' + convert(varchar, @QryQueryId) + ';' + CHAR(13) + CHAR(10) +
	'With checkdate AS' + CHAR(13) + CHAR(10) +

	'(Select PolicyId, sp.ParticipantId, ClmPaymentRequestId, CheckDate, count(CheckDate) as Counts, ClmSubmissionId,' + CHAR(13) + CHAR(10) +
	'ROW_NUMBER() over (Partition By ClmSubmissionId order by CheckDate ASC) as Sequence' + CHAR(13) + CHAR(10) +
	'From ClmPaymentRequest r' + CHAR(13) + CHAR(10) +
	'join ClmMDSLSpSubmission sp on sp.ClmMDSLSpSubmissionId = r.ClmSubmissionId' + CHAR(13) + CHAR(10) +
	'where CheckDate is not null ' + CHAR(13) + CHAR(10) +
	'group by PolicyId,ClmPaymentRequestId, CheckDate, ClmSubmissionId, sp.ParticipantId' + CHAR(13) + CHAR(10) +
	')' + CHAR(13) + CHAR(10) +

	'Insert into #ClaimCount (PolicyId, ParticipantId, ClmPaymentRequestId, ClmSubmissionId, ClaimIncurredDate, CIPart, CheckDate, CheckPart, ClaimCount, RowNumber)' + CHAR(13) + CHAR(10) +

	'Select src.PolicyId, ParticipantId, ClmPaymentRequestId, ClmSubmissionId, src.ClaimIncurredDate, dense_rank() over (order by src.ClaimIncurredDate) as CIPart, src.Checkdate,' + CHAR(13) + CHAR(10) +
	'dense_rank() over (order by CheckDate) as CheckPart, counts, ROW_NUMBER() OVER(Order by src.ClaimIncurredDate, Checkdate) as RowNumber' + CHAR(13) + CHAR(10) 

	Set @From = @From + CHAR(13) + CHAR(10) +
	'join' + CHAR(13) + CHAR(10) +
	'(' + CHAR(13) + CHAR(10) +
		'Select PolicyId, ParticipantId, ClmPaymentRequestId, ClmSubmissionId, CONVERT(datetime, ClaimIncurredDate + ''-01'') as ClaimIncurredDate, CONVERT(datetime, CheckDate + ''-01'') as CheckDate, counts' + CHAR(13) + CHAR(10) +
	    'From' + CHAR(13) + CHAR(10) +
		'(' + CHAR(13) + CHAR(10) +
		'Select PolicyId, s.ParticipantId, ClmPaymentRequestId, ClmsubmissionId, (convert(varchar,DATEPART(month,Checkdate))+ ''-''+convert(varchar,DATEPART(year,Checkdate))) as CheckDate,' + CHAR(13) + CHAR(10) +
		 '(convert(varchar,DATEPART(month,ClaimIncurredDate))+ ''-''+convert(varchar,DATEPART(year,ClaimIncurredDate))) as ClaimIncurredDate, counts' + CHAR(13) + CHAR(10) +
		'From checkdate c' + CHAR(13) + CHAR(10) +
		'left join ClmMDSLSpClaim cl on cl.ClmMDSLSpClaimId = c.ClmSubmissionId' + CHAR(13) + CHAR(10) +
		'left join ClmMDSLSpSubmission s on s.ClmMDSLSpSubmissionId = cl.ClmMDSLSpClaimId' + CHAR(13) + CHAR(10) +
		'where c.Sequence = 1 and ClaimIncurredDate is not null' + CHAR(13) + CHAR(10) +
		') a' + CHAR(13) + CHAR(10) +
	 ')src on src.PolicyId = blk.PolicyId' + CHAR(13) + CHAR(10) 

	select @OrderBy = ' Order By src.ClaimIncurredDate, src.CheckDate'
	select @where =  Case when @where is null or @where = '' then ' Where src.ClaimIncurredDate is not null'
	                 else 'Where' + @Where + ' and src.ClaimIncurredDate is not null'
					 End
	Select @Statement = @Statement + @From + @where + @Orderby 
 PRINT @Statement
	EXECUTE sp_executesql @Statement, N'@QueryId int, @UserId INT', @QryQueryId, @UserId

--- update the ClaimCount
   Update #ClaimCount
   Set ClaimCount = cc.ClaimCount
   From  (Select CIPart, CheckPart, sum(ClaimCount) as ClaimCount
   from #ClaimCount
   group by CheckPart, CIPart) cc
   where cc.CheckPart = #ClaimCount.CheckPart and cc.CIPart = #ClaimCount.CIPart



-- Now that we have the counts for each check date, par it down
-- selEct * from #NewClaimCount
--drop table #newClaimCount
Create Table #NewClaimCount
(ClaimIncurredDate datetime,
CIPart int,
CheckDate datetime,
Checkpart int,
ClaimCount int,
ClaimCountRatio numeric(10,2),
RowNumber int,
Lives int
) 

Insert into #NewClaimCount (ClaimIncurredDate, CIPart, CheckDate, CheckPart, ClaimCount, ClaimCountRatio, RowNumber, Lives)
Select distinct ClaimIncurredDate, CIPart, CheckDate, CheckPart, ClaimCount, null as ClaimCountRatio, null as RowNumber, null as lives
From #ClaimCount
order by ClaimIncurredDate


Update #newClaimCount
set RowNumber = nc.RN
From (select Checkdate, ClaimIncurredDate, row_number() over (Order by ClaimIncurredDate, CheckDate) as RN
From #newClaimCount) nc
where nc.ClaimIncurredDate = #newClaimCount.ClaimIncurredDate and nc.checkDate = #newClaimCount.CheckDate
--drop table #newClaimCount

--Update ClaimCountRatio

Declare @i int,
@maxrow int,
@rt int,
@ord int,
@datepartition int, -- Current Date Partition
@pdt int -- Date Partition of Previous Row


Select @i=2
Select @maxrow = max(rowNumber) from #newClaimCount
Select @rt = claimcount from #newClaimCount where RowNumber =1 
Select @ord = 1
;

Update #newClaimCount
set ClaimCountRatio = ClaimCount
where RowNumber = 1;

Set @DatePartition = 1
Set @pdt = 1
While @i <= @maxrow
Begin 

	
	update cc
    set @rt = Case when @pdt = @datepartition then  @rt + cc.ClaimCount when @pdt <> @datepartition then cc.ClaimCount End,
	@ord =  cc.RowNumber,
    ClaimCountRatio = @rt
	--select *
	from #newClaimCount cc
	join #newClaimCount cc2 on cc.RowNumber = cc2.RowNumber

	
	
	where cc.RowNumber = @i 
	and cc.RowNumber =  @ord +1
	--and rt.DatePartition = 1 and rt.DatePartition = 2--and rt.DatePartition in (1,2)--rt2.datePartition
	
	
	Set @i = @i +1
	Select @Datepartition = CIPart from #newClaimCount where RowNumber = @i
	Select @pdt = CIPart from #newClaimCount where RowNumber = @i -1;

End

--Select * from #newClaimCount

--select * from #ClaimEnrollment

Update #newClaimCount
Set ClaimCountRatio = (ClaimCountRatio/TotalEnrollmentByEffectiveDate) * 1000.00
,   Lives = TotalEnrollmentByEffectiveDate
from #newClaimCount
join #ClaimEnrollment ce on ce.EffectiveDate = #newClaimCount.ClaimIncurredDate


--Get all possible checkdates
-- drop table #temp
create table #temp
(

DatePartition int,
ClaimIncurredDate datetime,
[1] datetime,
[2] datetime, 
[3] datetime, 
[4] datetime, 
[5] datetime, 
[6] datetime, 
[7] datetime, 
[8] datetime, 
[9] datetime, 
[10] datetime, 
[11] datetime, 
[12] datetime, 
[13] datetime, 
[14] datetime, 
[15] datetime, 
[16] datetime, 
[17] datetime, 
[18] datetime, 
[19] datetime, 
[20] datetime, 
[21] datetime, 
[22] datetime, 
[23] datetime, 
[24] datetime, 
[25] datetime, 
[26] datetime, 
[27] datetime, 
[28] datetime
)
Insert into #temp (DatePartition, ClaimIncurredDate, [1], [2], [3], [4], [5], [6], [7], [8], [9], [10],
 [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], [26], [27], [28])
Select distinct  CIPart, ClaimIncurredDate, null, null, null, null, null, null, null, null, null, null, null, 
null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null
From #newClaimCount


  
Declare @a int,
@m int,
@Startdate datetime

Select @Startdate = ClaimIncurredDate from #temp where DatePartition =1
Select @a=1
Select @m = max(DatePartition) from #temp



While @a <= @m
Begin 

	
	update #temp
	set [1] =  @Startdate
	, [2] = dateadd(month, 1, @Startdate)
	, [3] = dateadd(month, 2, @Startdate) 
	, [4] = dateadd(month, 3, @Startdate)
	, [5] = dateadd(month, 4, @Startdate)
	, [6] = dateadd(month, 5, @Startdate)
	, [7] = dateadd(month, 6, @Startdate)
	, [8] = dateadd(month, 7, @Startdate)
	, [9] = dateadd(month, 8, @Startdate)
	, [10] = dateadd(month, 9, @Startdate)
	, [11] = dateadd(month, 10, @Startdate)
	, [12] = dateadd(month, 11, @Startdate)
	, [13] = dateadd(month, 12, @Startdate)
	, [14] = dateadd(month, 13, @Startdate)
	, [15] = dateadd(month, 14, @Startdate)
	, [16] = dateadd(month, 15, @Startdate)
	, [17] = dateadd(month, 16, @Startdate)
	, [18] = dateadd(month, 17, @Startdate)
	, [19] = dateadd(month, 18, @Startdate)
	, [20] = dateadd(month, 19, @Startdate)
	, [21] = dateadd(month, 20, @Startdate)
	, [22] = dateadd(month, 21, @Startdate)
	, [23] = dateadd(month, 22, @Startdate)
	, [24] = dateadd(month, 23, @Startdate)
	, [25] = dateadd(month, 24, @Startdate)
	, [26] = dateadd(month, 25, @Startdate)
	, [27] = dateadd(month, 26, @Startdate)
	, [28] = dateadd(month, 27, @Startdate)
	where DatePartition = @a
	
	

	Set @a = @a +1
	select @Startdate = ClaimIncurredDate From #temp where DatePartition = @a;
End

--select * from #temp

--create check partition based off of effective dates, unpivot to join
-- drop table #datemp
Select DatePartition, ClaimIncurredDate, CheckPart, CheckDates
into #datemp
From 
(Select DatePartition, ClaimIncurredDate, [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], [26], [27], [28]
from #temp) t
unpivot
(CheckDates for CheckPart in ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], [26], [27], [28])
) as unpvt


--select * from #datemp
--order by EffectiveDate


--select * from #NewClaimcount
--join unpivot with running total raw data to put correct/rolling checkpartitions in table
-- drop table #yetanothertemp
Select dt.DatePartition, dt.ClaimIncurredDate as ED, dt.CheckDates, dt.CheckPart, ClaimCount, ClaimCountRatio,Lives
into #yetanothertemp
from #newClaimCount rt
left join #datemp dt on dt.DatePartition = rt.CIPart and dt.ClaimIncurredDate = rt.ClaimIncurredDate
where rt.CheckDate = dt.CheckDates

--select * from #yetanothertemp

--order by checkpart DESC


--Select * from #ClaimCount
--order by datepartition, CheckPartition

-- set date equal to null if it's in the future, add running total

	Select convert(varchar,ED, 101) as ClaimIncurredDate, Lives
	, case when dateadd(month, 1, ED) < getdate()
	       then coalesce([1], 0) 
	  else null
	  end as [1]
	, case when dateadd(month, 2, ED) < getdate()
	       then coalesce([2],  [1], 0) 
	  else null
	  end as [2]
	, case when dateadd(month, 3, ED) < getdate()
	       then coalesce([3],  [2],  [1], 0) 
	  else null
	  end as [3]
	, case when dateadd(month, 4, ED) < getdate()
	       then coalesce([4],  [3],  [2],  [1], 0)
	  else null
	  end as [4]
	, case when dateadd(month, 5, ED) < getdate()
	       then coalesce([5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [5]
	, case when
 dateadd(month, 6, ED) < getdate()
	       then coalesce([6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [6]
	, case when dateadd(month, 7, ED) < getdate()
	       then coalesce([7],  [6],  [5],  [4],  [3],  [2],  [1], 0) 
	  else null
	  end as [7]
	, case when dateadd(month, 8, ED) < getdate()
	       then coalesce([8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
      else null 
	  end as [8]
	, case when dateadd(month, 9, ED) < getdate()
	       then coalesce([9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [9]
	, case when dateadd(month, 10, ED) < getdate()
	       then coalesce([10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
      else null
	  end as [10]
	, case when dateadd(month, 11, ED) < getdate()
	       then coalesce([11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [11]
	, case when dateadd(month, 12, ED) < getdate() 
	       then coalesce([12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [12]
	, case when dateadd(month, 13, ED) < getdate()
	       then coalesce([13], [12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [13]
	, case when dateadd(month, 14, ED) < getdate()
	       then coalesce([14], [13], [12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [14]
	, case when dateadd(month, 15, ED) < getdate()
	       then coalesce([15], [14], [13], [12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [15]
	, case when dateadd(month, 16, ED) < getdate()
	       then coalesce([16], [15], [14], [13], [12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [16]
	, case when dateadd(month, 17, ED) < getdate()
	       then coalesce([17], [16], [15], [14], [13], [12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [17]
	, case when dateadd(month, 18, ED) < getdate() 
	       then coalesce([18], [17], [16], [15], [14], [13], [12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [18]
	, case when dateadd(month, 19, ED) < getdate()
	       then coalesce([19], [18], [17], [16], [15], [14], [13], [12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null 
	  end as [19]
	, case when dateadd(month, 20, ED) < getdate()
	       then coalesce([20], [19], [18], [17], [16], [15], [14], [13], [12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2],  [1], 0)
	  else null
	  end as [20]
	, case when dateadd(month, 21, ED) < getdate()
	       then coalesce([21], [20], [19], [18], [17], [16], [15], [14], [13], [12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3],  [2], [1], 0)
	  else null
	  end as [21]
	, case when dateadd(month, 22, ED) < getdate() 
	       then coalesce([22], [21], [20], [19], [18], [17], [16], [15], [14], [13], [12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4],  [3], [2], [1], 0)
	  else null
	  end as [22]
	, case when dateadd(month, 23, ED) < getdate()
	       then coalesce([23], [22], [21], [20], [19], [18], [17], [16], [15], [14], [13], [12], [11], [10], [9],  [8],  [7],  [6],  [5],  [4], [3], [2], [1], 0)
	  else null
	  end as [23]
	, case when dateadd(month, 24, ED) < getdate() 
	       then coalesce([24], [23], [22], [21], [20], [19], [18], [17], [16], [15], [14], [13], [12], [11], [10], [9],  [8],  [7],  [6],  [5], [4], [3], [2], [1], 0)
	  else null
	  end as [24]
	, case when dateadd(month, 25, ED) < getdate()
	       then coalesce([25], [24], [23], [22], [21], [20], [19], [18], [17], [16], [15], [14], [13], [12], [11], [10], [9],  [8],  [7],  [6], [5], [4], [3], [2], [1],0)
	  else null
	  end as [25]
	, case when dateadd(month, 26, ED) < getdate() 
	       then coalesce([26], [25], [24], [23], [22], [21], [20], [19], [18], [17], [16], [15], [14], [13], [12], [11], [10], [9],  [8],  [7], [6], [5], [4], [3], [2], [1],0)
	  else null
	  end as [26]
	, case when dateadd(month, 27, ED) < getdate()
	       then coalesce([27], [26], [25], [24], [23], [22], [21], [20], [19], [18], [17], [16], [15], [14], [13], [12], [11], [10], [9],  [8], [7], [6], [5], [4], [3], [2], [1],0)
	  else null
	  end as [27]
	, case when dateadd(month, 28, ED) < getdate()
	       then coalesce([28], [27], [26], [25], [24], [23], [22], [21], [20], [19], [18], [17], [16], [15], [14], [13], [12], [11] , [10], [9], [8], [7], [6], [5], [4], [3], [2], [1],0) 
	  else null
	  end as [28]
	into #ClaimcountPivot
	from (
	Select ED,Lives,ClaimCountRatio, CheckPart
	from #yetanothertemp) rt 
	pivot
	(
	sum(ClaimCountRatio)
	for CheckPart in ([1], [2],[3],[4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], [26], [27], [28]) 
	)piv
	order by ED
	-- drop table #ClaimcountPivot

	Select * from #ClaimcountPivot

	SELECT COUNT(*)
	FROM tempdb.sys.columns
	WHERE object_id = object_id('tempdb..#ClaimcountPivot')




	DROP TABLE #ClaimCount
	DROP TABLE #yetanothertemp
	DROP TABLE #temp
	DROP TABLE #datemp
	DROP TABLE #newClaimCount
	DROP TABLE #ClaimcountPivot

END
GO



