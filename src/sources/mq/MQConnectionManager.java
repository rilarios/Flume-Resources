package co.bancolombia.flume.sources.mq;

import java.util.HashMap;
import java.util.Map;

import com.ibm.mq.MQQueueManager;
import com.ibm.mq.MQSimpleConnectionManager;

public class MQConnectionManager {
	private static MQConnectionManager instance = null;
	private static MQSimpleConnectionManager connManager = null;
	private Map<String, MQQueueManager> qmgrs = new HashMap<String, MQQueueManager>(0);
	
	private MQConnectionManager() {
	    connManager = new MQSimpleConnectionManager();
        connManager.setActive(MQSimpleConnectionManager.MODE_AUTO);
        connManager.setTimeout(5000);
        connManager.setMaxConnections(2);
        connManager.setMaxUnusedConnections(1);
	}

	public static synchronized MQConnectionManager getInstance() {
		if (instance == null) {
			instance = new MQConnectionManager();
		}

		return instance;
	}
	
	public MQQueueManager connect(String qmgrName) throws Exception {
		if (!qmgrs.containsKey(qmgrName)) {
			MQQueueManager tmpObj = new MQQueueManager(qmgrName, connManager);
			qmgrs.put(qmgrName, tmpObj);
		}
		
		MQQueueManager qmgr = qmgrs.get(qmgrName);
		
		return qmgr;
	}
	
	public void disconnect(MQQueueManager qmgr) throws Exception {
		if (qmgr != null) {
			qmgr.disconnect();
		}
	}
}

