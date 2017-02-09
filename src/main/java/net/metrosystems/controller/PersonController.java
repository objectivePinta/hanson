package net.metrosystems.controller;

import net.metrosystems.data.domain.Person;
import net.metrosystems.data.repository.PersonRepository;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController("person")
public class PersonController {

  private final PersonRepository personRepository;

  public PersonController(PersonRepository personRepository) {
    this.personRepository = personRepository;
  }

  @GetMapping(value = "person/{email}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
  public Person getPersonByEmail(@PathVariable("email") String email) {
    return personRepository.getPersonByEmail(email);
  }

  @GetMapping(value = "person", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
  public List<Person> getAll() {
    return personRepository.getAll();
  }

  @PostMapping(value = "person", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
  public void postNewPerson(@RequestBody Person person) {
    personRepository.savePerson(person);
  }

}
