package taxi;

import lombok.Builder;
import lombok.Data;

/**
 * @author Evgeny Borisov
 */
@Data
@Builder
public class TaxiOrder {
    private int id;
    private String city;
    private int km;
}
