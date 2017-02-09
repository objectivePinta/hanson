package net.metrosystems.data.repository;

import java.util.List;
import java.util.Optional;

import net.metrosystems.data.CassandraPersistenceService;
import net.metrosystems.data.accessors.PersonAccessor;
import net.metrosystems.data.domain.Person;
import org.springframework.stereotype.Repository;

import com.datastax.driver.mapping.Mapper;

@Repository
public class PersonRepository {
  private final CassandraPersistenceService persistenceService;
  private final PersonAccessor accessor;

  public PersonRepository(CassandraPersistenceService persistenceService) {
    this.persistenceService = persistenceService;
    this.accessor = persistenceService.getMappingManager().createAccessor(PersonAccessor.class);
  }

  public void savePerson(Person person) {
    Mapper<Person> mapper = persistenceService.getMappingManager().mapper(Person.class);
    mapper.save(person);
  }

  public Person getPersonByEmail(String email) {
    return accessor.getPersonByEmail(email);
  }

  public List<Person> getAll() {
    return accessor.getAllPersons().all();
  }

}
