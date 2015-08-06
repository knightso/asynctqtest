package asynctqtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.datastore.AsyncDatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;

public class TestServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	
	Logger logger = Logger.getLogger(TestServlet.class.getSimpleName());

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		logger.info("processing...");
		AsyncDatastoreService datastore = DatastoreServiceFactory
				.getAsyncDatastoreService();

		List<Future<?>> futures = new ArrayList<Future<?>>();
		for (int i = 0; i < 10; i++) {
			Transaction tx = null;
			try {
				tx = datastore.beginTransaction().get();
				
				String keyName = UUID.randomUUID().toString();
				Entity entity = new Entity("Task", keyName);
				entity.setProperty("status", "pending");
				entity.setProperty("createdAt", new Date());
				datastore.put(tx, entity);

				Queue queue = QueueFactory.getDefaultQueue();
				queue.addAsync(
						tx,
						TaskOptions.Builder.withUrl("/task")
								.param("key", keyName).method(Method.GET));

				futures.add(tx.commitAsync());
			} catch (Throwable t) {
				logger.severe("error: " + t.getMessage());
				t.printStackTrace();
				if (tx != null && tx.isActive()) {
					tx.rollback();
				}
				if (t instanceof RuntimeException) {
					throw (RuntimeException) t;
				}
				throw new IllegalStateException(t);
			}
		}

		for (Future<?> f : futures) {
			try {
				f.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new IllegalStateException(e);
			}
		}

		resp.setContentType("text/plain");
		resp.getWriter().println("OK\n\n");
	}
}
