IF EXISTS (
  SELECT id 
  FROM sysobjects
  WHERE type = 'P' AND [Name] = 'spSTDICertificate'
)
BEGIN
  DROP PROCEDURE [dbo].spSTDICertificate
END
GO 
--Begin Transaction
--exec spSTDICertificate 12345, null
--Rollback Transaction

--Begin Transaction
--exec spSTDICertificate 67891, null
--Rollback transaction

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
SET NOCOUNT ON
GO

CREATE PROCEDURE [dbo].[spSTDICertificate]
    @PlcPolicyId int, @UserId int
AS
BEGIN



Declare @Header varchar(max)

Declare @Body varchar(max)

Declare @Footer varchar(max)

Declare @EffectiveDatePlc varchar(max)

Declare @ExpirationDate varchar(max)

Declare @AnnDate varchar(max)

Declare @EffectiveDateClass varchar(max)

Declare @AccountName varchar(max)

Declare @State varchar(max)

Declare @PreExistingCondition1 varchar(max)

Declare @PreExistingCondition2 varchar(max)

Declare @PreExistingCondition3 varchar(max)

-- '<?xml version="1.0"?>
Set @Header ='<STDICertificateDataSource name=" Short-Term Disability">'

Set @Footer = '</STDICertificateDataSource>'

set @EffectiveDatePlc = (select convert(varchar(10),EffectiveDate, 101) from PlcPolicy where plcPolicyId = @PlcPolicyId)

set @ExpirationDate = (select convert(varchar(10),ExpirationDate, 101) from PlcPolicy where plcPolicyId = @PlcPolicyId)

set @AnnDate = (select convert(varchar(10),getdate(), 101))

set @EffectiveDateClass = (select top 1 convert(varchar(10),EffectiveDate, 101) from PlcClass where plcPolicyId = @PlcPolicyId)

set @AccountName = (select rtrim(AccountName) from PlcPolicy where plcPolicyId = @PlcPolicyId)

set @State = (Select rtrim(AccountState) from PlcPolicy where PlcPolicyId = @PlcPolicyId)

--set @PreExistingCondition1 = (select top 1 isnull(PreExistingConditionLookback1, '') from PlcSTDIClass s 
--								join PlcClass c on c.PlcClassId = s.PlcSTDIClassId
--								join PlcPolicy p on p.PlcPolicyId = c.plcPolicyId
--								where p.PlcPolicyId = @PlcPolicyId)
 
--set @PreExistingCondition2 = (select top 1 isnull(PreExistingConditionLookback2, '') from PlcSTDIClass s 
--								join PlcClass c on c.PlcClassId = s.PlcSTDIClassId
--								join PlcPolicy p on p.PlcPolicyId = c.plcPolicyId
--								where p.PlcPolicyId = @PlcPolicyId)

--set @PreExistingCondition3 = (select top 1 isnull(PreExistingConditionLookback3, '') from PlcSTDIClass s 
--								join PlcClass c on c.PlcClassId = s.PlcSTDIClassId
--								join PlcPolicy p on p.PlcPolicyId = c.plcPolicyId
--								where p.PlcPolicyId = @PlcPolicyId)

If Not Exists (Select PlcCLassId from PlcClass where PlcPolicyId = @PlcPolicyId)
Begin

		Select 
	       AccountName  = @AccountName
		 , AccountStreet = Record.AccountAddress1 
		 , Record.AccountCity
		 , AccountState = @State
		 , Record.AccountZip
		 , FederalId =  Record.AccountFederalId  
		 , Record.PolicyNo
		 , EffectiveDate = @EffectiveDatePlc
		 , ExpirationDate = @ExpirationDate
		 , AnniversaryDate = @AnnDate
		 , Record.GracePeriod
		 , IsParticipatingEntities = case
										When Exists (select pl.PlcPolicyId from PlcLocation pl where pl.PlcPolicyId = @PlcPolicyId)
										Then 'Y'
										Else 'N'
									End
		, W2Services = W2.W2Services
		, IsFICAMatch = Case splc.IsFICAMatch When 0 Then 'N' When 1 Then 'Y' End
		, IsERISAPlan = Case splc.IsERISAPlan When 0 Then 'N' When 1 Then 'Y' End
		, ERISAPlanNumber = isnull(splc.ERISAPlanNumber, '')
		From PlcPolicy Record
		join PlcSTDIPolicy splc on splc.PlcSTDIPolicyId = Record.PlcPolicyId
		join CodeDISIW2Services W2 on W2.W2Services = splc.W2Services
		where Record.PlcPolicyId = @PlcPolicyId
		FOR XML PATH ('STDIPolicy'), TYPE, ELEMENTS
END

Else
Begin

set @body = 
(
select
	(
		Select 
	       AccountName  = @AccountName
		 , AccountStreet = Record.AccountAddress1 
		 , Record.AccountCity
		 , AccountState = @State
		 , Record.AccountZip
		 , FederalId =  Record.AccountFederalId  
		 , Record.PolicyNo
		 , EffectiveDate = @EffectiveDatePlc
		 , ExpirationDate = @ExpirationDate
		 , AnniversaryDate = @AnnDate
		 , Record.GracePeriod
		 , IsParticipatingEntities = case
										When Exists (select pl.PlcPolicyId from PlcLocation pl where pl.PlcPolicyId = @PlcPolicyId)
										Then 'Y'
										Else 'N'
									End
		, W2Services = W2.W2Services
		, IsFICAMatch = Case splc.IsFICAMatch When 0 Then 'N' When 1 Then 'Y' End
		, IsERISAPlan = Case splc.IsERISAPlan When 0 Then 'N' When 1 Then 'Y' End
		, ERISAPlanNumber = isnull(splc.ERISAPlanNumber, '')
		  FOR XML PATH ('STDIPolicy'), TYPE, ELEMENTS
	),
	(Select 
	
			 pc.ClassNo
			, Description = pc.Description --Not sure if correct data
			, EffectiveDate = @EffectiveDateClass
			, psc.EligibilityRequirement
			, psc.EligibleHoursWorked
		    , IsUnion = Case psc.IsUnion When 0 Then 'N' When 1 Then 'Y' End
			, IsCafeteriaPlan = Case psc.IsCafeteriaPlan When 0 Then 'N' When 1 Then 'Y' End
			, Occupation = oc.Occupation
			, OccupationDescription = oc.Description
			, EmpLContributionPercent = psc.EmplContributionPercent
			, IsEvidenceOfInsurability = Case psc.EvidenceOfInsurability When 'NONE' Then 'N' Else 'Y' End
			, WaitPeriod = Case psc.WaitPeriodCurrent
						When 'ADOE' Then Replace(wp.Description, '<x>', psc.WaitPeriodCurrentInterval)
						When 'AMOE' Then Replace(wp.Description, '<x>', psc.WaitPeriodCurrentInterval)
						When 'CXDE' Then Replace(wp.Description, '<x>', psc.WaitPeriodCurrentInterval)
						When 'CXME' Then Replace(wp.Description, '<x>', psc.WaitPeriodCurrentInterval)
						When 'FDOE' Then Replace(wp.Description, '<x>', psc.WaitPeriodCurrentInterval)
						When 'FMOE' Then Replace(wp.Description, '<x>', psc.WaitPeriodCurrentInterval)
						Else wp.Description
					    End
		    , WaitPeriodNewHire = Case psc.WaitPeriodNewHire
						When 'ADOE' Then Replace(wp2.Description, '<x>', psc.WaitPeriodNewHireInterval)
						When 'AMOE' Then Replace(wp2.Description, '<x>', psc.WaitPeriodNewHireInterval)
						When 'CXDE' Then Replace(wp2.Description, '<x>', psc.WaitPeriodNewHireInterval)
						When 'CXME' Then Replace(wp2.Description, '<x>', psc.WaitPeriodNewHireInterval)
						When 'FDOE' Then Replace(wp2.Description, '<x>', psc.WaitPeriodNewHireInterval)
						When 'FMOE' Then Replace(wp2.Description, '<x>', psc.WaitPeriodNewHireInterval)
						Else wp2.Description
					    End
			, psc.BenefitBasis
			, psc.FlatBenefit
			, psc.BenefitPercentage
			, psc.MinimumBenefitBasis
			, psc.MinimumBenefit
			, MinimumBenefitPercent = psc.MinimumBenefitPercentage 
			, psc.MaximumBenefit
			, psc.GuaranteeIssue
			, BenefitCommenceInjury = bci.Description
			, BenefitCommenceSickness = bcs.Description
			, MaxBenefitDuration= mbd.Description
			, IsFirstDayHospitalization = Case psc.FirstDayHospitalization When 'INCL' Then 'Y' Else 'N' End
			, psc.SocialSecurityIntegration
			, psc.DisabilityDefinition
			, IsBenefitCalcRehab = Case psc.BenefitCalcRehab When 'INCL' Then 'Y' Else 'N' End
			, psc.PreDisabilityEarningsDef
			, IsPreDIEarningsInclBonus = Case psc.IsPreDIEarningsInclBonus When 0 Then 'N' When 1 Then 'Y' End
			, IsPreDIEarningsInclCommission= Case psc.IsPreDIEarningsInclCommission When 0 Then 'N' When 1 Then 'Y' End
			, IsPreDIEarningsInclTipsTokens = Case psc.IsPreDIEarningsInclTipsTokens When 0 Then 'N' When 1 Then 'Y' End
			, IsContinuationFML = Case psc.ContinuationFML When 'NONE' Then 'N' Else 'Y' End
			, ContinuationFMLPeriod = psc.ContinuationFMLInterval
			, IsContinuationLayOff = Case psc.ContinuationLayOff When 'NOTI' Then 'N' Else 'Y' End
			, IsContinuationLOA = Case psc.ContinuationLOA When 'NOTI' Then 'N' Else 'Y' End
			, IsContinuationMilitary = Case psc.ContinuationMilitary When 'NOTI' Then 'N' Else 'Y' End
			, ContinuationMilitaryPeriod = psc.ContinuationMilitaryInterval
			, PreExistingConditionLimit = psc.PreExistingConditionLimit
			, PreExistingConditionLookback1 =Case When charindex('/',pre.description) <>3 and substring(pre.Description, 2,1) <> 'x' Then substring(pre.Description, 1,1)
													When pre.description = '<x>/<y>' then isnull(convert(varchar, psc.PreExistingConditionLookback1), '')
													When pre.description = '<x>/<y>/<z>' then isnull(convert(varchar,psc.PreExistingConditionLookback1), '')
												Else  substring(pre.Description,1,2)
											End
			, PreExistingConditionLookback2 = Case When CHARINDEX('/', pre.description, 1)  = 2 and CHARINDEX('/', pre.description, 3) = 4 and substring(pre.Description, 2,1) <> 'x' Then Substring(pre.description, 3,1)
												   When CHARINDEX('/', pre.description, 1)  = 3 and CHARINDEX('/', pre.description, 4) = 5 and substring(pre.Description, 2,1) <> 'x' Then Substring(pre.description, 4,1)
												   When CHARINDEX('/', pre.description, 1)  = 2 and CHARINDEX('/', pre.description, 3) = 5 and substring(pre.Description, 2,1) <> 'x' Then Substring(pre.description, 3,2)
												   When CHARINDEX('/', pre.description, 1)  = 3 and CHARINDEX('/', pre.description, 4) = 6 and substring(pre.Description, 2,1) <> 'x' Then Substring(pre.description, 4,2)
												   When pre.description = '<x>/<y>' then isnull(convert(varchar,psc.PreExistingConditionLookback2), '')
												   When pre.description = '<x>/<y>/<z>' then isnull(convert(varchar,psc.PreExistingConditionLookback2), '')
												End							
		    , PreExistingConditionLookback3 = Case When CHARINDEX('/', pre.description, 1)  = 2 and CHARINDEX('/', pre.description, 3) = 4 and len(pre.description) =5 and substring(pre.Description, 2,1) <> 'x' Then Substring(pre.description, 5,1)
												   When CHARINDEX('/', pre.description, 1)  = 3 and CHARINDEX('/', pre.description, 4) = 5 and len(pre.description) =6 and substring(pre.Description, 2,1) <> 'x' Then Substring(pre.description, 6,1)
												   When CHARINDEX('/', pre.description, 1)  = 3 and CHARINDEX('/', pre.description, 4) = 6 and len(pre.description) =7 and substring(pre.Description, 2,1) <> 'x' Then Substring(pre.description, 7,1)
												   When CHARINDEX('/', pre.description, 1)  = 2 and CHARINDEX('/', pre.description, 3) = 4 and len(pre.description) =6 and substring(pre.Description, 2,1) <> 'x' Then Substring(pre.description, 5,2)
												   When CHARINDEX('/', pre.description, 1)  = 3 and CHARINDEX('/', pre.description, 4) = 5 and len(pre.description) =7 and substring(pre.Description, 2,1) <> 'x' Then Substring(pre.description, 6,2)
												   When CHARINDEX('/', pre.description, 1)  = 3 and CHARINDEX('/', pre.description, 4) = 6 and len(pre.description) =8 and substring(pre.Description, 2,1) <> 'x' Then Substring(pre.description, 7,2)
												   When pre.description = '<x>/<y>' then isnull(convert(varchar,psc.PreExistingConditionLookback3), '')
												   When pre.description = '<x>/<y>/<z>' then isnull(convert(varchar,psc.PreExistingConditionLookback3), '')
												End
			, PreExistingConditionBftDuration = isnull(bft.Description, '')
			, IsMentalNervousLimitation = Case psc.MentalNervousLimit When 'NOTI' Then 'N' Else 'Y' End
		    , MentalNervousLimitation = mnl.MentalNervousLimit
			, IsSubstanceAbuseLimitation = Case psc.SubstanceabuseLimit When 'NOTI' Then 'N' Else 'Y' End
			, SubstanceAbuseLimitation = sal.SubstanceabuseLimit 
			, IsShiftDifferential = Case psc.IsShiftDifferential When 0 Then 'N' When 1 Then 'Y' End FOR XML PATH ('STDIClass'), TYPE, ELEMENTS
			),
			  (Select(Select 
					  Name = isnull(pl.Name, '')
					, EffectiveDate = isnull(convert(varchar,pl.EffectiveDate), '')
					, ExpirationDate = isnull(convert(varchar, pl.ExpirationDate), '')
					FOR XML PATH ('Entity'),Type, Elements
					 )
			  FOR XML PATH ('STDIParticipatingEntities'), TYPE, Elements
			 )
From PlcPolicy Record
join PlcClass pc on pc.PlcPolicyId = Record.PlcPolicyId
join PlcSTDIPolicy splc on splc.PlcSTDIPolicyId = Record.PlcPolicyId
join PlcSTDIClass psc on psc.PlcSTDIClassId = pc.PlcClassId
join CodeDISIWaitPeriod wp on wp.WaitPeriod = psc.WaitPeriodCurrent
join CodeDISIWaitPeriod wp2 on wp2.WaitPeriod = psc.WaitPeriodNewHire
join CodeDISIOccupation oc on oc.Occupation = psc.Occupation
join CodeDISIPreExistingConditionLookback pre on pre.PreExistingConditionLookback = psc.PreExistingConditionLookback
join CodeSTDIBenefitCommenceInjury bci on bci.BenefitCommenceInjury = psc.BenefitCommenceInjury
join CodeSTDIBenefitCommenceSickness bcs on bcs.BenefitCommenceSickness = psc.BenefitCommenceSickness
join CodeSTDIMaximumBenefitDuration mbd on mbd.MaximumBenefitDuration = psc.MaximumBenefitDuration
join CodeSTDIMentalNervousLimit mnl on mnl.MentalNervousLimit = psc.MentalNervousLimit
join CodeSTDISubstanceabuseLimit sal on sal.SubstanceabuseLimit = psc.SubstanceabuseLimit
join CodeSTDIPreExistingConditionBnftDuration bft on bft.PreExistingConditionBnftDuration = psc.PreExistingConditionBnftDuration
join CodeDISIW2Services W2 on W2.W2Services = splc.W2Services
left join PlcLocation pl on pl.PlcPolicyId = Record.PlcPolicyId
where Record.PlcPolicyID = @PlcPolicyId --7035455
For XML Auto, Elements
)
select  convert(xml, @Header + Replace(Replace ({"Attachments":[{"__type":"ItemIdAttachment:#Exchange","ItemId":{"__type":"ItemId:#Exchange","ChangeKey":null,"Id":"AAMkADE0NzQ5NGNkLTRkZWMtNGFmYy04MzgyLTIxOWI1ZjI0YWM1NQBGAAAAAADtsoJHN/DsRZa19jMGxfe/BwBdt2KOGUQERIrwAJAEah9fAAAAAAEJAABdt2KOGUQERIrwAJAEah9fAAADzTFOAAA="},"Name":"STD Cert Class 1.doc - Central Distributors","IsInline":false}]}@Body, '<STDIClass>', '<STDIClass name = "Class">'),'<STDIPolicy>','<STDIPolicy name="Policy">')  + @Footer)
END
End