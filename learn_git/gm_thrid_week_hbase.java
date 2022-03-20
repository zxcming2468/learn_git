private static final Logger logger = LoggerFactiory.getLogger(HBaseOperation.class);
public static final String OP_ROW_KEY = "ZhangYu";
private static Connection connection = null;
private static Admin admin = null;

static{
	try{
		Configuration configuration = HBaseConfiguration.create();
		configuration.set{"hbase.zookeeper.quorum","emr-worker-2,emr-worker-1,emr-header-1"};
		configuration.set{"hbae.zookeeper.property.clientPort","2181"};
		connection = ConnectonFactiory.createConnection(Configuration);
		admin = connection.getAdmin();
	} catch (IOException e){
		logger.error("init failed",e);
	}
}

public static boolean createTable(String tableName,String...columnFamilies){
	if(StringUtils.isEmpty(tableName) || columnFamilies.length<1){
		throw now IllegalArgumentException("tableName or columnFamilies is null");
	}
	TableDescriptorBuilder tDescBuilder = HBaseConfiguration
		TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
	for(String columnFamily:columnFamiles){
		ColumnFamilyDescriptor descriptor = 
			ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build();
		tDescBuilder.setColumnFamily(descriptor);
} try{
	admin.createTable(tDescBuilder.build());
	logger.info("createTable success, tableName:()",tableName,e);
}
return false;
}

public static void deleteTable(String tableName) throws IOException{
	admin.disableTable(TableName.valueOf(tableName));
	admin.deleteTable(TableName.valueOf(tableName));
	logger.info("deleteTable success, tableName:{}",tableName);
}

public static void putData(String tableName, String rowKey, String colFamily,
		String colKey, String colValue) throws IOException{
	Table table = connection.getTable(TableName.valueOf(tableName));
	Put put = new Put(Bytes.toBytes(rowKey));
	put.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(colKey),Bytes.toBytes(colValue));
	table.put(put);
	table.close();
}

public static void getData(String tableName, String rowKey, String colFamily,String colKey) throws IOException{
	Table table = connection.getTable(TableName.valueOf(tableName));
	Get get = new Get(Bytes.toBytes(rowKey));
	if(StringUtils.isEmpty(colKey)){
		get.addFamily(Bytes.toBytes(colFamily));
	}else{
		get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(colKey));
	}
	Result result = table.get(get);
	for (Cell cell : result.rawCells()){
		String family = Bytes.toString(CellUtil.cloneFamily(cell));
		String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
		String value = Bytes.toString(CellUtil.cloneValue(cell));
		logger.info("Family:{},Qualifier:{},valuez:{}",family,qualifier,value);
	}
	table.close();
}


public static void scanTable(String tableName) throws IOException{
	Table table = connection.getTable.valueOf(tableName);
	Scan scan = new Scan();
	ResultScanner resultScanner = table.getScanner(scan);
	for (Result result:resultScanner){
		for(Cell cell:result.rawCells()){
			String row = Bytes.toString(CellUtil.cloneRow(cell));
			String family = Bytes.toString(CellUtil.cloneQualifier(cell));
			String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
			String value = Bytes.toString(CellUtil.cloneValue(cell));
			logger.info("Row:{},Family:{},Qualifier:{},Value:{}",row,family,qualifier,value);
		}
	}
}

public static void deleteData(String tableName,String rowKey, String colFamily,String colKey) throws IOException{
	Table table = connection.getTable(TableName.valueOf(tableName));
	Delete delete = new Delete(Bytes.toBytes(rowKey));
	delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(colKey));

	atble.delete(delete);
}