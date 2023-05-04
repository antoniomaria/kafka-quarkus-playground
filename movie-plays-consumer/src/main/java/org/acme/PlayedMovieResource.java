package org.acme;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Multi;

@Path("/movies")
public class PlayedMovieResource {

    @Channel("played-movies")
    Multi<PlayedMovie> playedMovies;

    @GET
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public Multi<PlayedMovie> stream() {
        return playedMovies;
    }
}
