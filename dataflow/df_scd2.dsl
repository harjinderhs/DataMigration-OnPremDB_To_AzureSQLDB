parameters{
	file_name as string
}
source(output(
		member_id as short,
		member_name as string,
		member_email as string,
		member_phone as long
	),
	useSchema: false,
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'delimited',
	fileSystem: 'mycontainer',
	folderPath: 'onPrem_LibraryDB/Members',
	fileName: ($file_name),
	columnDelimiter: ',',
	escapeChar: '\\',
	quoteChar: '\"',
	columnNamesAsHeader: true) ~> AdlsCSVSource
source(output(
		memberId as integer,
		hashKey as long
	),
	allowSchemaDrift: true,
	validateSchema: false,
	format: 'query',
	store: 'sqlserver',
	query: 'Select  memberId, hashKey from dbo.Members_SCDTYPE2 where isActive=1',
	isolationLevel: 'READ_UNCOMMITTED') ~> Target
AdlsCSVSource select(mapColumn(
		each(match(1==1),
			concat('src_',$$) = $$)
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> Rename
Rename derive(src_HashKey = crc32(concat(toString(src_member_id), src_member_name, src_member_email, toString(src_member_phone)))) ~> GenerateHashKey
GenerateHashKey, Target lookup(src_member_id == memberId,
	multiple: false,
	pickup: 'any',
	broadcast: 'auto')~> lookupTarget
lookupTarget split(isNull(memberId),
	src_member_id==memberId && src_HashKey!=hashKey,
	disjoint: false) ~> Split@(Insert, Update)
Split@Update derive(src_updatedby = 'DataFlow-Updated',
		src_updateddate = currentTimestamp(),
		src_isActive = 0) ~> AuditUpdateColumns
AuditUpdateColumns alterRow(updateIf(1==1)) ~> alterRow
Split@Insert, Split@Update union(byName: true)~> union
union derive(src_createdby = 'DataFlow',
		src_createddate = currentTimestamp(),
		src_updatedby = 'DataFlow',
		src_updateddate = currentTimestamp(),
		src_isActive = 1) ~> InsertAuditColumns
alterRow sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		memberId as integer,
		memberName as string,
		memberEmail as string,
		memberPhone as string,
		createdBy as string,
		createdDate as timestamp,
		updatedBy as string,
		updatedDate as timestamp,
		hashkey as long,
		isActive as integer
	),
	format: 'table',
	store: 'sqlserver',
	schemaName: 'dbo',
	tableName: 'Members_SCDTYPE2',
	insertable: false,
	updateable: true,
	deletable: false,
	upsertable: false,
	keys:['memberId','hashkey'],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	errorHandlingOption: 'stopOnFirstError',
	mapColumn(
		memberId,
		updatedBy = src_updatedby,
		updatedDate = src_updateddate,
		hashkey = hashKey,
		isActive = src_isActive
	)) ~> SinkUpdate
InsertAuditColumns sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		memberId as integer,
		memberName as string,
		memberEmail as string,
		memberPhone as string,
		createdBy as string,
		createdDate as timestamp,
		updatedBy as string,
		updatedDate as timestamp,
		hashkey as long,
		isActive as integer
	),
	format: 'table',
	store: 'sqlserver',
	schemaName: 'dbo',
	tableName: 'Members_SCDTYPE2',
	insertable: true,
	updateable: false,
	deletable: false,
	upsertable: false,
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	errorHandlingOption: 'stopOnFirstError',
	mapColumn(
		memberId = src_member_id,
		memberName = src_member_name,
		memberEmail = src_member_email,
		memberPhone = src_member_phone,
		createdBy = src_createdby,
		createdDate = src_createddate,
		updatedBy = src_updatedby,
		updatedDate = src_updateddate,
		hashkey = src_HashKey,
		isActive = src_isActive
	)) ~> SinkInsert