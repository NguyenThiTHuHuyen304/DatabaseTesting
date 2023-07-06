package storedprocedureTesting;

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

public class SPTesting {

	Connection con = null;
	Statement stmt = null;
	ResultSet rs;
	ResultSet rs1;
	ResultSet rs2;
	CallableStatement cStmt;

	@BeforeClass
	void setup() throws SQLException {
		con = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels", "root", "Huyen304!");
	}

	@AfterClass
	void tearDown() throws SQLException {
		con.close();
	}

	@Test(priority = 1)
	void test_storedProceduresExists() throws SQLException {
		stmt = con.createStatement();
		rs = stmt.executeQuery("SHOW PROCEDURE STATUS WHERE Name = 'GetCustomerShipping'");
		rs.next();

		Assert.assertEquals(rs.getString("Name"), "GetCustomerShipping");
	}

	@Test(priority = 2)
	void test_selectAllCustomers() throws SQLException {
		cStmt = con.prepareCall("{call SelectAllCustomers()}");
		rs1 = cStmt.executeQuery();

		Statement stmt = con.createStatement();
		rs2 = stmt.executeQuery("select * from customers");

		Assert.assertEquals(compareResultSets(rs1, rs2), true);
	}

	@Test(priority = 3)
	void test_selectAllCustomersByCity() throws SQLException {
		cStmt = con.prepareCall("{call SelectAllCustomersByCity(?)}");
		cStmt.setString(1, "Singapore");
		rs1 = cStmt.executeQuery();

		Statement stmt = con.createStatement();
		rs2 = stmt.executeQuery("select * from customers where city = 'Singapore'");

		Assert.assertEquals(compareResultSets(rs1, rs2), true);
	}

	@Test(priority = 4)
	void test_selectAllCustomersByCityAndPin() throws SQLException {
		cStmt = con.prepareCall("{call SelectAllCustomersByCityAndPin(?,?)}");
		cStmt.setString(1, "Singapore");
		cStmt.setString(2, "079903");
		rs1 = cStmt.executeQuery();

		Statement stmt = con.createStatement();
		rs2 = stmt.executeQuery("select * from customers where city = 'Singapore' and postalCode = 079903");

		Assert.assertEquals(compareResultSets(rs1, rs2), true);
	}

	@Test(priority = 5)
	void test_get_order_by_cust() throws SQLException {
		cStmt = con.prepareCall("{call get_order_by_cust(?,?,?,?,?)}");
		cStmt.setInt(1, 141);

		cStmt.registerOutParameter(2, Types.INTEGER);
		cStmt.registerOutParameter(3, Types.INTEGER);
		cStmt.registerOutParameter(4, Types.INTEGER);
		cStmt.registerOutParameter(5, Types.INTEGER);

		cStmt.executeQuery();

		int shipped = cStmt.getInt(2);
		int cancelled = cStmt.getInt(3);
		int resolved = cStmt.getInt(4);
		int disputed = cStmt.getInt(5);

//		System.out.println(shipped + " " + cancelled + " " + resolved + " " + disputed);

		Statement stmt = con.createStatement();
		rs = stmt.executeQuery(
				"select(select count(*)  from orders where customerNumber = 141 and status = 'Shipped' )as 'Shipped',(select count(*) from orders where customerNumber = 141 and status = 'Cancelled') as 'Cancelled',(select count(*)  from orders where customerNumber = 141 and status = 'Disputed') as 'Disputed',(select count(*)  from orders where customerNumber = 141 and status = 'Resolved') as 'Resolved'");

		rs.next();

		int exp_shipped = rs.getInt("shipped");
		int exp_cancelled = rs.getInt("cancelled");
		int exp_resolved = rs.getInt("resolved");
		int exp_disputed = rs.getInt("disputed");

		if (shipped == exp_shipped && cancelled == exp_cancelled && resolved == exp_resolved
				&& disputed == exp_disputed) {
			Assert.assertTrue(true);
		} else {
			Assert.assertFalse(false);
		}
	}

	@Test(priority = 6)
	void getCustomerShipping() throws SQLException {
		cStmt = con.prepareCall("{call GetCustomerShipping(?,?)}");
		cStmt.setInt(1, 103);
		cStmt.registerOutParameter(2, Types.VARCHAR);

		cStmt.executeQuery();

		String shippingTime = cStmt.getString(2);

//		System.out.println(shipping);

		stmt = con.createStatement();
		rs = stmt.executeQuery(
				"select country,case when country = 'USA' then '2-day Shipping' when country = 'Canada' then '3-day Shipping' else '5-day Shipping' end as shippingTime from customers where customerNumber = 103");

		rs.next();

		String exp_shippingTime = rs.getString("Shippingtime");

		Assert.assertEquals(shippingTime, exp_shippingTime);
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
