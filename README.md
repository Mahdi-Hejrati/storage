# storage
Just another ORM for C# and WPF for fast APP development

How to Use

Storage.Sql.SqlService.DefaultInstance = new Storage.Sql.SqlService(ConnectionString);


Query Data


	var result = Storage.Sql.SqlService.DefaultInstance.Xq(TableName)
		.setfields("fieldname", "realdbfieldsname AS field2", "some expersion as field3")
		.And("z.field", valuetocompare)
		.Or("z.field2", value2, Op.Gt)
		.join_lo("LEFT OUTER JOIN t2 ON t2.id=z.t2id")
		.Select();

	//result is the query results 

	foreach(var q in result){
		q.getAs<long>("fieldname", defaultvalue);
		q.getAs<string>("field2");
	}


	// add Converter and Calculateable fields

	result.Adapter.registerFormatter(new DataFormatter(){
		Field="fieldname",
		Format="formattername",
		Forward = // forward converter
		Backward = // backward converter
	});


##To be continued






