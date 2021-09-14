package demo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author: guangxush
 * @create: 2019/05/18
 */
public class ServiceCenter implements Server {

    private static ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private static final HashMap<String, Class> serviceRegistry = new HashMap<String, Class>();

    private static boolean isRunning = false;

    private static int port;


    public ServiceCenter(int port) {
        ServiceCenter.port = port;
    }


    @Override
    public void start() throws IOException {
        ServerSocket server = new ServerSocket();
        server.bind(new InetSocketAddress(port));
        System.out.println("demo.Server Start .....");
        try {
            while (true) {
                executor.execute(new ServiceTask(server.accept()));
            }
        } finally {
            server.close();
        }
    }

    @Override
    public void register(Class serviceInterface, Class impl) {
        serviceRegistry.put(serviceInterface.getName(), impl);
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void stop() {
        isRunning = false;
        executor.shutdown();
    }

    private static class ServiceTask implements Runnable {
        Socket client;

        public ServiceTask(Socket client) {
            this.client = client;
        }

        @Override
        public void run() {
            ObjectInputStream input = null;
            ObjectOutputStream output = null;
            try {
                System.out.println("getting input ...");
                input = new ObjectInputStream(client.getInputStream());
                System.out.println("catching input ...");
                String serviceName = input.readUTF();  // 读出类名
                String methodName = input.readUTF();  // 读出方法名
                Class<?>[] parameterTypes = (Class<?>[]) input.readObject();  // 读出方法参数类型
                Object[] arguments = (Object[]) input.readObject();  // 读出方法输入对象
                Class<?> serviceClass = serviceRegistry.get(serviceName);  // 由 类名 获得 类的class对象
                if (serviceClass == null) {
                    throw new ClassNotFoundException(serviceName + "not found!");
                }
                Method method = serviceClass.getMethod(methodName, parameterTypes);  // 获取相应方法
                Object result = method.invoke(serviceClass.newInstance(), arguments);  // 反射调用方法

                output = new ObjectOutputStream(client.getOutputStream());
                output.writeObject(result);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (output != null) {
                    try {
                        output.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (input != null) {
                    try {
                        input.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (client != null) {
                    try {
                        client.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
