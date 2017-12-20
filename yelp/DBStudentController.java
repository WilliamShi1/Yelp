package yelp;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is the class file you will have to modify. You should only have to modify this file
 * and nothing else.
 * 
 * You will have to connect to the yelp.db database given. Once you are connected to the database,
 * you can execute queries on that database. The result will be return in a ResultSet. Fill in the 
 * appropriate method for each query. 
 * 
 * @author 
 *
 */

/**
 * Below are some snippets of JDBC code that may prove useful
 * 
 * For more sample JDBC code, check out 
 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
 * 
 * ---
 * 
 *      // INITIALIZE THE CONNECTION
 *      Class.forName("org.sqlite.JDBC");
 *      Connection conn = DriverManager.getConnection("jdbc:sqlite:PATH_TO_DB_FILE");
 * ---
 * 
 * Using PreparedStatement:
 * 
 * public void someQuery(String businessID){
 * 		String query = "SELECT * from business WHERE id = ? ;";
 * 		PreparedStatement prep = conn.prepareStatement(query);
 * 		prep.setString(1, businessID);
 * 		ResultSet rs = prep.executeQuery();
 * 		while (rs.next()) {
 * 			System.out.println("id = " + rs.getString("id"));
 * 			System.out.println("name = " + rs.getString("name"));
 * 		}
 * 		rs.close();
 * }
 * 
 */

public class DBStudentController implements DBController {
	Connection conn;
	public DBStudentController() throws SQLException, ClassNotFoundException {
		// Initialize the connection.
		Class.forName("org.sqlite.JDBC");
		conn = DriverManager.getConnection("jdbc:sqlite:./yelp.db");
	}

	/**
	 * This function is called for query 1
	 * 
	 * Get the businesses in Providence, RI that are still open. 
	 * Results should be sorted by review counts in descending order. Return top 7 businesses.
	 * 
	 * @input N/A
	 * @output Six columns - the business id, name, full address, review count, photo url, and stars of the business.
	 * 
	 * @return A List of BusinessObject containing the result to the query.  
	 * @throws SQLException
	 */
	@Override
	public List<BusinessObject> query1() throws SQLException {
		// Your code goes here. Refer to BusinessObject.java
		// FOR FULL CREDIT make sure to set the id, name, address, reviewCount, photoUrl and stars properties of your BusinessObjects
		String query = "SELECT b.id, b.name, b.full_address, b.review_count, b.photo_url, b.stars "
				+ "FROM business as b "
				//+ "JOIN review as r ON b.id = r.business_id "		
				+ "WHERE b.city = 'Providence' AND b.state = 'RI' "
				+ "GROUP BY b.id "
				+ "ORDER BY b.review_count DESC "
				+ "LIMIT 7;";
					 			   
		PreparedStatement prep = conn.prepareStatement(query);
		ResultSet rs = prep.executeQuery();
		List<BusinessObject> result = new ArrayList<BusinessObject>();
				
		while (rs.next()) {
			BusinessObject current = new BusinessObject();
			current.setId(rs.getString("id"));
			current.setName(rs.getString("name"));
			current.setAddress(rs.getString("full_address"));
			current.setReviewCount(Integer.parseInt(rs.getString("review_count")));
			current.setPhotoUrl(rs.getString("photo_url"));
			current.setStars(Double.valueOf(rs.getString("stars")));
			result.add(current);
		}
		rs.close();
		return result;
		// throw new UnsupportedOperationException();
	}

	/**
	 * This function is called for query 2
	 * 
	 * Get the reviews for a particular business, given the business ID. 
	 * Results should be sorted by the review's useful vote counts in descending order. Return top 7 reviews.
	 * 
	 * @input businessID
	 * @output Four columns - the user id, name of the user, stars of the review, and text of the review.
	 * 
	 * 
	 * @return A List of ReviewObject containing the result to the query
	 * @throws SQLException
	 */
	@Override
	public List<ReviewObject> query2(String businessID) throws SQLException {
		// Your code goes here. Refer to ReviewObject.java
		// FOR FULL CREDIT make sure to set the id, name, stars, text properties of your ReviewObjects
		String query = "SELECT u.id, u.name, r.stars, r.text "
				+ "FROM review as r "
				+ "JOIN user as u ON u.id = r.user_id "
				+ "JOIN business as b ON b.id = r.business_id "		
				+ "WHERE b.id = ? "
				+ "ORDER BY r.useful_votes DESC "
				+ "LIMIT 7;";
					 	
		List<ReviewObject> result = new ArrayList<ReviewObject>();
		PreparedStatement prep = conn.prepareStatement(query);
		prep.setString(1, businessID);
		ResultSet rs = prep.executeQuery();
				
		while (rs.next()) {
			ReviewObject current = new ReviewObject();
			current.setId(rs.getString("id"));
			current.setName(rs.getString("name"));
			current.setText(rs.getString("text"));
			current.setStars(Double.valueOf(rs.getString("stars")));
			result.add(current);
		}
		rs.close();
		return result;
		
		//throw new UnsupportedOperationException();
	}

	/**
	 * This function is called for query 3
	 * 
	 * Find the average star rating across all reviews written by a particular user.
	 * 
	 * @input userID
	 * @output One columns - the average star rating.
	 * 
	 * @return the average star rating
	 * @throws SQLException
	 */
	@Override
	public double query3(String userID) throws SQLException {
		// Your code goes here.
		
		String query = "SELECT avg(stars) "
				+ "FROM review as r "
				+ "WHERE r.user_id = ? ;";
					 	
		double result = 0;
		PreparedStatement prep = conn.prepareStatement(query);
		prep.setString(1, userID);
		ResultSet rs = prep.executeQuery();
				
		while (rs.next()) {
			result = Double.valueOf(rs.getString("avg(stars)"));
		}
		rs.close();
		return result;
		//throw new UnsupportedOperationException();
	}

	/**
	 * This function is called for query 4
	 * 
	 * Get the businesses in Providence, RI that have been reviewed by more than 5 'elite' users. 
	 * Users who have written more than 10 reviews are called 'elite' users. 
	 * Results should be ordered by the 'elite' user count in descending order. Return top 7 businesses.
	 * 
	 * @input N/A
	 * @output Seven columns - the business id, business name, business full address, review count, photo url, stars, and the count of the 'elite' users for the particular business.
	 * 
	 * @return A List of BusinessObject representing the results to the query.
	 * @throws SQLException
	 */
	@Override
	public List<BusinessObject> query4() throws SQLException {
		// Your code goes here. Refer to BusinessObject.java
		// FOR FULL CREDIT make sure to set the id, name, address, reviewCount, photoUrl, stars, and elite count properties of your BusinessObjects
		
		String query = "SELECT b.id, b.name, b.full_address, b.review_count, b.photo_url, b.stars, count(distinct r.user_id) as countElite "
				+ "FROM business as b "
				+ "JOIN review as r ON r.business_id = b.id "
				+ "WHERE b.city = 'Providence' AND b.state = 'RI' AND r.user_id IN"
				+ "(SELECT id "
				+ "FROM user "
				+ "WHERE review_count > 10) "
				+ "GROUP BY r.business_id "
				+ "HAVING countElite > 5 "
				+ "ORDER BY countElite DESC "
				+ "LIMIT 7 ;";
		
		List<BusinessObject> result = new ArrayList<BusinessObject>();
		PreparedStatement prep = conn.prepareStatement(query);
		ResultSet rs = prep.executeQuery();
				
		while (rs.next()) {
			BusinessObject current = new BusinessObject();
			current.setId(rs.getString("id"));
			current.setName(rs.getString("name"));
			current.setAddress(rs.getString("full_address"));
			current.setReviewCount(Integer.parseInt(rs.getString("review_count")));
			current.setPhotoUrl(rs.getString("photo_url"));
			current.setStars(Double.valueOf(rs.getString("stars")));
			current.setEliteCount(Integer.parseInt(rs.getString("countElite")));
			result.add(current);
		}
		
		rs.close();
		return result;
		//throw new UnsupportedOperationException();
	}

	/**
	 * This function is called for query 5
	 * 
	 * Get the businesses in Providence, RI that have the highest percentage of five star reviews, and have been reviewed at least 20 times.
	 * Results should be ordered by the percentage in descending order. Return top 7 businesses.
	 * 
	 * @input N/A
	 * @output Seven columns - the business id, business name, business full address, review count, photo url, stars, and percentage of five star reviews
	 * 
	 * @return A List of BusinessObject representing the results to the query.
	 * @throws SQLException
	 */
	@Override
	public List<BusinessObject> query5() throws SQLException {
		// Your code goes here. Refer to BusinessObject.java
		// FOR FULL CREDIT make sure to set the id, name, address, reviewCount, photoUrl, stars, and percentage properties of your BusinessObjects
		String query = "SELECT b.id, b.name, b.full_address, b.review_count, b.photo_url, b.stars, count(r.stars)*1.00/b.review_count as percentage "
				+ "FROM business as b "
				+ "JOIN review as r ON r.business_id = b.id "
				+ "WHERE b.city = 'Providence' AND b.state = 'RI' AND b.review_count > 20 AND r.stars = 5 "
				+ "GROUP BY r.business_id "
				+ "ORDER BY percentage DESC "
				+ "LIMIT 7;";
		
		List<BusinessObject> result = new ArrayList<BusinessObject>();
		PreparedStatement prep = conn.prepareStatement(query);
		ResultSet rs = prep.executeQuery();
				
		while (rs.next()) {
			BusinessObject current = new BusinessObject();
			current.setId(rs.getString("id"));
			current.setName(rs.getString("name"));
			current.setAddress(rs.getString("full_address"));
			current.setReviewCount(Integer.parseInt(rs.getString("review_count")));
			current.setPhotoUrl(rs.getString("photo_url"));
			current.setStars(Double.valueOf(rs.getString("stars")));
			current.setPercentage(Double.valueOf(rs.getString("percentage")));
			result.add(current);
		}
		
		rs.close();
		return result;
		//throw new UnsupportedOperationException();
	}
}