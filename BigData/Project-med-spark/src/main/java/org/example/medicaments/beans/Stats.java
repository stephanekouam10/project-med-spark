package org.example.medicaments.beans;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
@Builder
public class Stats implements Serializable {
    private String VoieKey = "";
    private String SubstanceCount = "0";
}
