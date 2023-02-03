package org.example.medicaments.beans;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
@Builder

public class Medicament implements Serializable {

    private String substance;
    private String voie;
    private String statut;
}

