package co.bancolombia.flume.sources.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ExamplePool {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		ConnectionPool pool 
		= new ConnectionPool(2,2, "com.ibm.as400.access.AS400JDBCDriver" , "jdbc:as400://10.9.2.201/NACIONAL"
				, "NCXOMONR", "DSCM0NR1"); 
		
		Connection conn = pool.getConnection();
		PreparedStatement ps;
		ResultSet resultado;
		
		String q = "SELECT * FROM PCCLIBRAMD.PCCFFLGAUD " +
		"  WHERE cast ( concat( VARCHAR('20'||(RIGHT(RIGHT(CONCAT('0',FECHARCV),6),2))||(LEFT(RIGHT(CONCAT ('0',FECHARCV),6),2))||RIGHT((LEFT(RIGHT(CONCAT('0',FECHARCV),6),4)),2)) ,  lpad(VARCHAR(HORARCV) , 8, '0') ) as bigint) > 2017022114380000" 
				+ " AND (OPERACION = 50  "                                                             
				+ " AND SUBSTR(DATOS,152,9) LIKE '%4610%'   "
				+ " OR SUBSTR(DATOS,152,9)  LIKE '%4611%'   "
				+ " OR SUBSTR(DATOS,152,9)  LIKE '%5643%'   "
				+ " OR SUBSTR(DATOS,152,9)  LIKE '%44563%') ";

		System.out.println("Q >" + q);
		ps = conn.prepareStatement(q);
		resultado = ps.executeQuery();
		int cols = resultado.getMetaData().getColumnCount();
		int j = 1;
		while(resultado.next()){
			String val = "";
			for(int i=1;i<=cols;i++){

				val = val + "," + resultado.getString(i);
			}
			System.out.println(j + "=>" + val);
			j+=1;
		}
		
		pool.closeAllConnections();

	}

}
