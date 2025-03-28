parameters{
	file_name as string
}
source(output(
		book_id as short,
		book_title as string,
		book_author as string,
		book_genre as string,
		published_year as short
	),
	useSchema: false,
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'delimited',
	fileSystem: 'mycontainer',
	folderPath: 'onPrem_LibraryDB/Books',
	fileName: ($file_name),
	columnDelimiter: ',',
	escapeChar: '\\',
	quoteChar: '\"',
	columnNamesAsHeader: true,
	multiLineRow: true) ~> AdlsCSVSource
source(output(
		bookId as integer,
		hashkey as long
	),
	allowSchemaDrift: true,
	validateSchema: false,
	format: 'query',
	store: 'sqlserver',
	query: 'Select bookId, hashkey from dbo.Books_SCDTYPE1',
	isolationLevel: 'READ_UNCOMMITTED') ~> Target
AdlsCSVSource select(mapColumn(
		each(match(1==1),
			concat('src_',$$) = $$)
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> Rename
Rename derive(src_Hashkey = crc32(concat(toString(src_book_id), src_book_title, src_book_author, src_book_genre,toString(src_published_year)))) ~> Hashkey
Hashkey, Target lookup(src_book_id == bookId,
	multiple: false,
	pickup: 'any',
	broadcast: 'auto')~> lookup
lookup split(isNull(bookId),
	src_book_id == bookId && hashkey != src_Hashkey,
	disjoint: true) ~> split@(Insert, Update)
split@Insert derive(src_createdby = 'DataFlow',
		src_createddate = currentTimestamp(),
		src_updatedby = 'DataFlow',
		src_updateddate = currentTimestamp()) ~> InsertAuditColumns
split@Update derive(src_updatedby = 'Dataflow-Updated',
		src_updateddate = currentTimestamp()) ~> UpdateAuditColumns
UpdateAuditColumns alterRow(updateIf(1==1)) ~> alterRow
InsertAuditColumns sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		bookId as integer,
		bookTitle as string,
		bookAuthor as string,
		bookGenre as string,
		publishedYear as integer,
		createdBy as string,
		createdDate as timestamp,
		updatedBy as string,
		updatedDate as timestamp,
		hashkey as long
	),
	format: 'table',
	store: 'sqlserver',
	schemaName: 'dbo',
	tableName: 'Books_SCDTYPE1',
	insertable: true,
	updateable: false,
	deletable: false,
	upsertable: false,
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	errorHandlingOption: 'stopOnFirstError',
	mapColumn(
		bookId = src_book_id,
		bookTitle = src_book_title,
		bookAuthor = src_book_author,
		bookGenre = src_book_genre,
		publishedYear = src_published_year,
		createdBy = src_createdby,
		createdDate = src_createddate,
		updatedBy = src_updatedby,
		updatedDate = src_updateddate,
		hashkey = src_Hashkey
	)) ~> SinkInsert
alterRow sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		bookId as integer,
		bookTitle as string,
		bookAuthor as string,
		bookGenre as string,
		publishedYear as integer,
		createdBy as string,
		createdDate as timestamp,
		updatedBy as string,
		updatedDate as timestamp,
		hashkey as long
	),
	format: 'table',
	store: 'sqlserver',
	schemaName: 'dbo',
	tableName: 'Books_SCDTYPE1',
	insertable: false,
	updateable: true,
	deletable: false,
	upsertable: false,
	keys:['bookId'],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	errorHandlingOption: 'stopOnFirstError',
	mapColumn(
		bookId = src_book_id,
		bookTitle = src_book_title,
		bookAuthor = src_book_author,
		bookGenre = src_book_genre,
		publishedYear = src_published_year,
		updatedBy = src_updatedby,
		updatedDate = src_updateddate,
		hashkey = src_Hashkey
	)) ~> SinkUpdate