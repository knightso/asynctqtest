package asynctqtest;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import asynctqtest.StatsServlet.Result.Task;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.QueueStatistics;

public class StatsServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	
	Logger logger = Logger.getLogger(StatsServlet.class.getSimpleName());

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {

		Result result = new Result();

		Queue defaultQueue = QueueFactory.getDefaultQueue();
		QueueStatistics queueStats = defaultQueue.fetchStatistics();
		
		result.setExecutedLastMinute(queueStats.getExecutedLastMinute());
		result.setNumTasks(queueStats.getNumTasks());
		result.setRequestsInFlight(queueStats.getRequestsInFlight());
		
		
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		
		Query query = new Query("Task").addSort("createdAt", SortDirection.DESCENDING);
		PreparedQuery pq = datastore.prepare(query);
		List<Entity> entities = pq.asList(FetchOptions.Builder.withLimit(20));
		for (Entity entity : entities) {
			Task task = new Result.Task();
			task.setStatus((String)entity.getProperty("status"));
			task.setCreatedAt((Date)entity.getProperty("createdAt"));
			result.getTaskEntities().add(task);
		}
		
		resp.setContentType("application/json");
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setCharacterEncoding("utf-8");
		
		try (PrintWriter rw = resp.getWriter()) {
			ObjectMapper om = new ObjectMapper();
			om.enable(SerializationConfig.Feature.INDENT_OUTPUT);
			om.writeValue(rw, result);
		}
	}
	
	public static class Result implements Serializable {

		private static final long serialVersionUID = 1L;

		private long executedLastMinute;
		private int numTasks;
		private int requestsInFlight;
		private List<Task> taskEntities = new ArrayList<Task>();
		
		public long getExecutedLastMinute() {
			return executedLastMinute;
		}
		public void setExecutedLastMinute(long executedLastMinute) {
			this.executedLastMinute = executedLastMinute;
		}
		public int getNumTasks() {
			return numTasks;
		}
		public void setNumTasks(int numTasks) {
			this.numTasks = numTasks;
		}
		public int getRequestsInFlight() {
			return requestsInFlight;
		}
		public void setRequestsInFlight(int requestsInFlight) {
			this.requestsInFlight = requestsInFlight;
		}
		public List<Task> getTaskEntities() {
			return taskEntities;
		}
		public void setTaskEntities(List<Task> taskEntities) {
			this.taskEntities = taskEntities;
		}

		public static class Task implements Serializable {

			private static final long serialVersionUID = 1L;
			
			private String status;
			private Date createdAt;
			public String getStatus() {
				return status;
			}
			public void setStatus(String status) {
				this.status = status;
			}
			public Date getCreatedAt() {
				return createdAt;
			}
			public void setCreatedAt(Date createdAt) {
				this.createdAt = createdAt;
			}
		}
	}
}
