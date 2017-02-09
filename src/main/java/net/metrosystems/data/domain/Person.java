package net.metrosystems.data.domain;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "person")
@Builder
public class Person {

  @Column(name = "email")
  @PartitionKey // echivaleaza aproximativ cu primary key din sql
  String email;

  @Column(name = "first_name")
  String firstName;

  @Column(name = "last_name")
  String lastName;

  @Column(name = "address")
  String address;

}
