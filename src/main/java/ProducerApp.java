import kafka.utils.json.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class Ordre{
    public String type;
    public String societe;
    public int nbActions;
    public Collection<Double> prixAction = new ArrayList<>();

    public Ordre(String type, String societe, int nbActions){
        this.type=type;
        this.societe=societe;
        this.nbActions=nbActions;
    }

    @Override
    public String toString() {
        String str = "{'Societe':'"+societe+"', 'Type':'"+type+"', 'NbActions':"+nbActions+", 'Prix':"+prixAction+"}";
        return  str;
    }
}

public class ProducerApp {
    private int counter=0;
    public static Map<Integer, String> societes = new HashMap<>();
    public static Map<Integer, String> types = new HashMap<>();


    public ProducerApp(){
        for(int i=1; i<7; i++) societes.put(i, "Societe"+i);
        types.put(1, "Achat");
        types.put(2, "Vente");

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        Random random= new Random();
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{

            Ordre ordre = new Ordre(types.get(random.nextInt(2)+1) , societes.get(random.nextInt(6)+1) , random.nextInt(100));
            for(int i=0; i<ordre.nbActions;i++)
                ordre.prixAction.add(random.nextDouble()*1000);

            String key=String.valueOf(++counter);
            String value=ordre.toString();

            kafkaProducer.send(new ProducerRecord<String, String>("newTopic2",key,value),(metadata,ex)->{
                System.out.println("Sending message: "+value+" | Partition: "+metadata.partition()+" | Offset: "+metadata.offset());
            });
        },10,10, TimeUnit.MILLISECONDS);

    }
    public static void main(String[] args) {
        new ProducerApp();
    }
}
