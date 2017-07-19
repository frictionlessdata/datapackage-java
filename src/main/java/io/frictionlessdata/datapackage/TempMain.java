
import io.frictionlessdata.datapackage.DataPackage;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.everit.json.schema.ValidationException;

/**
 *
 */
public class TempMain {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        System.out.println("Initial setup of library.");
       
        
        try{
            //String sourceFileAbsPath = TempMain.class.getResource("/tests/fixtures/not_a_json_datapackage.json").getPath();
            //DataPackage dp = new DataPackage(sourceFileAbsPath, null);
            
            //DataPackage dp = new DataPackage(new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-php/master/tests/fixtures/fake/simple_invalid_datapackage.json"));
        
           

            //DataPackage dp = new DataPackage(url);
            //System.out.println(dp.getJSONObject());
        
            
        } catch (ValidationException ve) {
            // prints validation errors
            System.out.println(ve.getMessage());
            ve.getCausingExceptions().stream()
                .map(ValidationException::getMessage)
                .forEach(System.out::println);
            
        }catch(Exception e){
            e.printStackTrace();
        }
        
        System.out.println("Done.");
        
    }
    
}
