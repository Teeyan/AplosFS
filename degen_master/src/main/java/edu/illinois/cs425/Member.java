package degenfs;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.stereotype.Component;


import java.io.Serializable;
import java.time.Instant;

@Component
@Getter
@Setter
@AllArgsConstructor
public class Member implements Serializable {

   private String id;
   private Instant lastAlive;

   // Needed for Spring to not complain.
   public Member() {

   }

   public String toString() {
   		return id;
   }
}
