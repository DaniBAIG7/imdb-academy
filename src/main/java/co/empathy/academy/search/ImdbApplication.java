package co.empathy.academy.search;

import co.empathy.academy.search.util.tsv_merger.TSVMerger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class ImdbApplication {

	public static void main(String[] args) {
		SpringApplication.run(ImdbApplication.class, args);
		try {
			TSVMerger.cleanAkas("src/main/resources/cleaning/title.basics.sorted.cleaned.tsv",
					"src/main/resources/cleaning/title.akas.sorted.tsv",
					"src/main/resources/cleaning/title.akas.sorted.cleaned.tsv");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
