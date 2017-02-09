package net.metrosystems.data.accessors;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;

import net.metrosystems.data.domain.Person;

@Accessor
public interface PersonAccessor {

  @Query("select * FROM person WHERE email=:email")
  Person getPersonByEmail(@Param("email") String email);

  @Query("select * FROM person")
  Result<Person> getAllPersons();

  @Query("delete FROM person WHERE email=:email")
  void deletePersonByEmail(@Param("email") String email);
}
