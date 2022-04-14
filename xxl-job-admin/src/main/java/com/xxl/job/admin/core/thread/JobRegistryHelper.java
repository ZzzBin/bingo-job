package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobRegistry;
import com.xxl.job.admin.dao.XxlJobGroupDao;
import com.xxl.job.admin.dao.XxlJobRegistryDao;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.*;

/**
 * job registry instance
 * @author xuxueli 2016-10-02 19:10:24
 */
public class JobRegistryHelper {
	private static Logger logger = LoggerFactory.getLogger(JobRegistryHelper.class);

	private static JobRegistryHelper instance = new JobRegistryHelper();
	public static JobRegistryHelper getInstance(){
		return instance;
	}

	private ThreadPoolExecutor registryOrRemoveThreadPool = null;
	private Thread registryMonitorThread;
	private ThreadPoolExecutor pingThreadPool = null;
	private Thread pingMonitorThread = null;
	private volatile boolean toStop = false;

	public void start(){
		pingThreadPool = new ThreadPoolExecutor(2, 10, 30L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(2000),
				(r, executor) -> r.run());

		pingMonitorThread = new Thread(this::refresh, "pingMonitorThread");

		pingMonitorThread.setDaemon(true);
		pingMonitorThread.start();
		if (pingThreadPool != null) {
			return;
		}

		// for registry or remove
		registryOrRemoveThreadPool = new ThreadPoolExecutor(
				2,
				10,
				30L,
				TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(2000),
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						return new Thread(r, "xxl-job, admin JobRegistryMonitorHelper-registryOrRemoveThreadPool-" + r.hashCode());
					}
				},
				new RejectedExecutionHandler() {
					@Override
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						r.run();
						logger.warn(">>>>>>>>>>> xxl-job, registry or remove too fast, match threadpool rejected handler(run now).");
					}
				});

		// for monitor
		registryMonitorThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!toStop) {
					try {
						// auto registry group
						List<XxlJobGroup> groupList = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().findByAddressType(0);
						if (groupList!=null && !groupList.isEmpty()) {

							// remove dead address (admin/executor)
							List<Integer> ids = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
							if (ids!=null && ids.size()>0) {
								XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().removeDead(ids);
							}

							// fresh online address (admin/executor)
							HashMap<String, List<String>> appAddressMap = new HashMap<String, List<String>>();
							List<XxlJobRegistry> list = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
							if (list != null) {
								for (XxlJobRegistry item: list) {
									if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
										String appname = item.getRegistryKey();
										List<String> registryList = appAddressMap.get(appname);
										if (registryList == null) {
											registryList = new ArrayList<String>();
										}

										if (!registryList.contains(item.getRegistryValue())) {
											registryList.add(item.getRegistryValue());
										}
										appAddressMap.put(appname, registryList);
									}
								}
							}

							// fresh group address
							for (XxlJobGroup group: groupList) {
								List<String> registryList = appAddressMap.get(group.getAppname());
								String addressListStr = null;
								if (registryList!=null && !registryList.isEmpty()) {
									Collections.sort(registryList);
									StringBuilder addressListSB = new StringBuilder();
									for (String item:registryList) {
										addressListSB.append(item).append(",");
									}
									addressListStr = addressListSB.toString();
									addressListStr = addressListStr.substring(0, addressListStr.length()-1);
								}
								group.setAddressList(addressListStr);
								group.setUpdateTime(new Date());

								XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().update(group);
							}
						}
					} catch (Exception e) {
						if (!toStop) {
							logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
						}
					}
					try {
						TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
					} catch (InterruptedException e) {
						if (!toStop) {
							logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
						}
					}
				}
				logger.info(">>>>>>>>>>> xxl-job, job registry monitor thread stop");
			}
		});
		registryMonitorThread.setDaemon(true);
		registryMonitorThread.setName("xxl-job, admin JobRegistryMonitorHelper-registryMonitorThread");
		registryMonitorThread.start();
	}

	public void toStop(){
		toStop = true;

		// stop registryOrRemoveThreadPool
		registryOrRemoveThreadPool.shutdownNow();

		// stop monitir (interrupt and wait)
		registryMonitorThread.interrupt();
		try {
			registryMonitorThread.join();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}


	// ---------------------- helper ----------------------

	public ReturnT<String> registry(RegistryParam registryParam) {

		// valid
		if (!StringUtils.hasText(registryParam.getRegistryGroup())
				|| !StringUtils.hasText(registryParam.getRegistryKey())
				|| !StringUtils.hasText(registryParam.getRegistryValue())) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
		}

		// async execute
		registryOrRemoveThreadPool.execute(new Runnable() {
			@Override
			public void run() {
				int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registryUpdate(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue(), new Date());
				if (ret < 1) {
					XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registrySave(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue(), new Date());

					// fresh
					freshGroupRegistryInfo(registryParam);
				}
			}
		});

		return ReturnT.SUCCESS;
	}

	public ReturnT<String> registryRemove(RegistryParam registryParam) {

		// valid
		if (!StringUtils.hasText(registryParam.getRegistryGroup())
				|| !StringUtils.hasText(registryParam.getRegistryKey())
				|| !StringUtils.hasText(registryParam.getRegistryValue())) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
		}

		// async execute
		registryOrRemoveThreadPool.execute(new Runnable() {
			@Override
			public void run() {
				int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().registryDelete(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue());
				if (ret > 0) {
					// fresh
					freshGroupRegistryInfo(registryParam);
				}
			}
		});

		return ReturnT.SUCCESS;
	}

	private void freshGroupRegistryInfo(RegistryParam registryParam){
		// Under consideration, prevent affecting core tables
	}

	// --------------------------- bingo ------------------------------------

	/**
	 * 定时刷新执行器地址列表
	 */
	public void refresh() {
		while (!toStop) {
			boolean interrupted = Thread.currentThread().isInterrupted();
			if (interrupted) {
				logger.warn("refresh has been interrupted");
				break;
			}
			try {
				// 5秒执行一次
				TimeUnit.SECONDS.sleep(5);
				// 获取最近10s内的register信息来刷新执行器地址列表
//				logger.warn("refresh");
				Date date = new Date();
				List<XxlJobRegistry> allRegistry = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findAll(10, date);
				Map<String, StringBuilder> appName2Address = new HashMap<>();
				for (XxlJobRegistry xxlJobRegistry : allRegistry) {
					StringBuilder address = appName2Address.computeIfAbsent(xxlJobRegistry.getRegistryKey(), key -> new StringBuilder());
					address.append(xxlJobRegistry.getRegistryValue()).append(",");
				}
				XxlJobGroupDao xxlJobGroupDao = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao();
				List<XxlJobGroup> allGroup = xxlJobGroupDao.findByAddressType(0);
				for (XxlJobGroup xxlJobGroup : allGroup) {
					StringBuilder address = appName2Address.get(xxlJobGroup.getAppname());
					// 30s内没有执行器ping
					if (address == null) {
						xxlJobGroup.setAddressList(null);
					} else {
						xxlJobGroup.setAddressList(address.substring(0, address.length() - 1));
					}
					xxlJobGroup.setUpdateTime(date);
					xxlJobGroupDao.update(xxlJobGroup);
				}
			} catch (InterruptedException e) {
				logger.warn("refresh has been interrupted");
				toStop = true;
			} catch (Exception e) {
				logger.error("refresh exception", e);
			}
		}
	}

	/**
	 *	心跳检测：executor定时ping调度中心，若超过一定时间还没ping则kill掉此executor
	 */
	public ReturnT<String> ping(RegistryParam registryParam) {
		String registryGroup = registryParam.getRegistryGroup();
		String registryKey = registryParam.getRegistryKey();
		String registryValue = registryParam.getRegistryValue();
		if (!StringUtils.hasText(registryGroup) || !StringUtils.hasText(registryKey) || !StringUtils.hasText(registryValue)) {
			return ReturnT.fail("参数错误");
		}
		Date date = new Date();
		XxlJobRegistryDao xxlJobRegistryDao = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao();
		int length = xxlJobRegistryDao.registryUpdate(registryGroup, registryKey, registryValue, date);
		if (length == 1) {
			return ReturnT.SUCCESS;
		}
		length = xxlJobRegistryDao.registrySave(registryGroup, registryKey, registryValue, date);
		if (length == 1) {
			return ReturnT.SUCCESS;
		}
		logger.warn("fail to save register {} {} {} {}", registryGroup, registryKey, registryValue, date);
		return ReturnT.fail("服务器繁忙");
	}
}
