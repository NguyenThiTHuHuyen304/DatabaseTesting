package storedfunctionTesting;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SFTesting {

	Connection con = null;
	Statement stmt;
	CallableStatement cStmt;
	ResultSet rs;
	ResultSet rs1;
	ResultSet rs2;

	@BeforeClass
	void setup() throws SQLException {
		con = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels", "root", "Huyen304!");
	}

	@AfterClass
	void tearDown() throws SQLException {
		con.close();
	}

	@Test(priority = 1)
	void test_storedFunctionExists() throws SQLException {
		stmt = con.createStatement();
		rs = stmt.executeQuery("SHOW function status WHERE DB= 'classicmodels'");
		rs.next();

		Assert.assertEquals(rs.getString("Name"), "CustomerLevel");
	}

	@Test(priority = 2)
	void test_CustomerLevel_with_SQLStatement() throws SQLException {
		rs1 = con.createStatement().executeQuery("select customerName, CustomerLevel(creditLimit) from customers");
		rs2 = con.createStatement().executeQuery(
				"select customerName, case when creditLimit > 50000 then 'PLATINUM' when creditLimit <= 50000 and creditLimit >= 10000 then 'GOLD' when creditLimit < 10000 then 'SILVER'end as customerlevel from customers");

		Assert.assertTrue(compareResultSets(rs1, rs2));
	}

	@Test(priority = 3)
	void test_CustomerLevel_with_StoredProcedure() throws SQLException {
		rs1 = con.createStatement().executeQuery(
				"select customerName, case when creditLimit > 50000 then 'PLATINUM' when creditLimit <= 50000 and creditLimit >= 10000 then 'GOLD' when creditLimit < 10000 then 'SILVER'end as customerlevel from customers where customerNumber = 103");
		rs1.next();
		String exp_customerLevel = rs1.getString("customerlevel");

		cStmt = con.prepareCall("{call getCustomerLevel(?,?)}");
		cStmt.setInt(1, 103);
		cStmt.registerOutParameter(2, Types.VARCHAR);
		cStmt.executeQuery();

		String customerLevel = cStmt.getString(2);

		Assert.assertEquals(exp_customerLevel, customerLevel);
	}

	public boolean compareResultSets(ResultSet resultSet1, ResultSet resultSet2) throws SQLException {
		while (resultSet1.next()) {
			resultSet2.next();

			int count = resultSet1.getMetaData().getColumnCount();
			for (int i = 1; i <= count; i++) {
				if (!StringUtils.equals(resultSet1.getString(i), resultSet2.getString(i))) {
					return false;
				}
			}
		}

		return true;
	}
}
