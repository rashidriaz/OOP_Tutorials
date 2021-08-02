package db.music.java.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataSource {
    public static final String DB_NAME = "music.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:D:\\OOP_Tutorials\\Music\\" + DB_NAME;
    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;

    public static final String TABLE_ARTIST = "artists";
    public static final String COLUMN_ARTIST_ID = "_id";
    public static final String COLUMN_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;

    public static final String TABLE_SONG = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_TRACK = 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_ALBUM = 4;

    public static final int ORDER_BY_NONE = 1;
    public static final int ORDER_BY_ASC = 2;
    public static final int ORDER_BY_DESC = 3;

    public static final String QUERY_ALBUMS_BY_ARTIST_START =
            "SELECT " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " FROM " + TABLE_ALBUMS +
                    " INNER JOIN " + TABLE_ARTIST + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " +
                    TABLE_ARTIST + "." + COLUMN_ARTIST_ID +
                    " WHERE " + TABLE_ARTIST + "." + COLUMN_ALBUM_NAME + " = \"";
    public static final String QUERY_ALBUMS_BY_ARTIST_SORT =
            " ORDER BY " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";
    public static final String QUERY_ARTIST_OR_SONG_START =
            "SELECT " + TABLE_ARTIST + "." + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " +
                    TABLE_SONG + "." + COLUMN_SONG_TRACK + " FROM " + TABLE_SONG + " INNER JOIN " +
                    TABLE_ALBUMS + " ON " + TABLE_SONG + "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
                    " INNER JOIN " + TABLE_ARTIST + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " +
                    TABLE_ARTIST + "." + COLUMN_ARTIST_ID + " WHERE " + TABLE_SONG + "." + COLUMN_SONG_TITLE + " = \"";
    public static final String QUERY_ARTIST_OR_SONG_SORT =
            " ORDER BY " + TABLE_ARTIST + "." + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME +
                    " COLLATE NOCASE ";

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";
    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " +
            TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTIST + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONG_ALBUM + ", " +
            TABLE_SONG + "." + COLUMN_SONG_TRACK + ", " + TABLE_SONG + "." + COLUMN_SONG_TITLE +
            " FROM " + TABLE_SONG + " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONG + "." + COLUMN_SONG_ALBUM +
            " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID + " INNER JOIN " + TABLE_ARTIST + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST +
            " = " + TABLE_ARTIST + "." + COLUMN_ARTIST_ID + " ORDER BY " + TABLE_ARTIST + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " +
            TABLE_SONG + "." + COLUMN_SONG_TRACK;


    public static final String QUERY_VIEW_SONG_INFO = "SELECT " + COLUMN_ARTIST_NAME + ", " + COLUMN_SONG_ALBUM + ", " +
            COLUMN_SONG_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW +
            " WHERE " + COLUMN_SONG_TITLE + " = \"";

    public static final String QUERY_SONG_INFO_PREP = "SELECT " + COLUMN_ARTIST_NAME + ", " +
            COLUMN_SONG_ALBUM + ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW +
            " WHERE " + COLUMN_SONG_TITLE + " = ?";

    public static final String INSERT_ARTIST = "INSERT INTO " + TABLE_ARTIST +
            '(' + COLUMN_ARTIST_NAME + ") VALUES(?)";
    public static final String INSERT_ALBUMS = "INSERT INTO " + TABLE_ALBUMS +
            '(' + COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTIST + ") VALUES(?, ?)";
    public static final String INSERT_SONGS = "INSERT INTO " + TABLE_SONG +
            '(' + COLUMN_SONG_TRACK + ", " + COLUMN_SONG_TITLE + ", " + COLUMN_SONG_ALBUM + ") VALUES(?, ?, ?)";

    public static final String QUERY_ARTIST = "SELECT " + COLUMN_ARTIST_ID + " FROM " +
            TABLE_ARTIST + " WHERE " + COLUMN_ARTIST_NAME + " = ?";
    public static final String QUERY_ALBUM = "SELECT " + COLUMN_ALBUM_ID + " FROM " +
            TABLE_ALBUMS + " WHERE " + COLUMN_ALBUM_NAME + " = ?";
    public static final String QUERY_SONG = "SELECT " + COLUMN_SONG_ID + " FROM " +
            TABLE_SONG + " WHERE " + COLUMN_SONG_TITLE + " = ?";

    private Connection connection;
    private PreparedStatement querySongInfoView;
    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;
    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;
    private PreparedStatement querySong;


    public boolean open() {
        try {
            connection = DriverManager.getConnection(CONNECTION_STRING);
            querySongInfoView = connection.prepareStatement(QUERY_SONG_INFO_PREP);
            insertIntoArtists = connection.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = connection.prepareStatement(INSERT_ALBUMS, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = connection.prepareStatement(INSERT_SONGS);
            queryArtist = connection.prepareStatement(QUERY_ARTIST);
            queryAlbum = connection.prepareStatement(QUERY_ALBUM);
            querySong = connection.prepareStatement(QUERY_SONG);
            return true;
        } catch (SQLException e) {
            System.out.println("Couldn't connect to database |" + e.getMessage());
            return false;
        }
    }

    public void close() {
        try {
            if (querySongInfoView != null) {
                querySongInfoView.close();
            }
            if (insertIntoArtists != null) {
                insertIntoArtists.close();
            }
            if (insertIntoAlbums != null) {
                insertIntoAlbums.close();
            }
            if (insertIntoSongs != null) {
                insertIntoSongs.close();
            }
            if (queryArtist != null) {
                queryArtist.close();
            }
            if (queryAlbum != null) {
                queryAlbum.close();
            }
            if (querySong != null) {
                querySong.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.out.println("Couldn't close the database |" + e.getMessage());
        }
    }

    public List<Artist> queryArtist(int sortOrder) {
        StringBuilder queryString = new StringBuilder("SELECT * FROM ");
        queryString.append(TABLE_ARTIST);
        if (sortOrder != ORDER_BY_NONE) {
            queryString.append(" ORDER BY ");
            queryString.append(COLUMN_ARTIST_NAME);
            queryString.append(" COLLATE NOCASE");
            if (sortOrder == ORDER_BY_ASC) {
                queryString.append(" ASC");
            }
            if (sortOrder == ORDER_BY_DESC) {
                queryString.append(" DESC");
            }
        }

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(queryString.toString())) {

            List<Artist> artists = new ArrayList<>();
            while (results.next()) {
                Artist artist = new Artist();
                artist.setId(results.getInt(INDEX_ARTIST_ID));
                artist.setName(results.getString(INDEX_ARTIST_NAME));
                artists.add(artist);
            }
            return artists;
        } catch (SQLException e) {
            System.out.println("Query Failed | " + e.getMessage());
            return null;
        }
    }

    public List<String> queryAlbumsForArtist(String artistName, int sortOrder) {
        StringBuilder query = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
        query.append(artistName);
        query.append("\"");
        query.append(QUERY_ALBUMS_BY_ARTIST_SORT);
        if (sortOrder == ORDER_BY_ASC) {
            query.append(" ASC");
        }
        if (sortOrder == ORDER_BY_DESC) {
            query.append(" DESC");
        }
        System.out.println(query.toString() +
                "\n-----------------------------------------------------------------------");

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(query.toString())) {

            List<String> albums = new ArrayList<>();
            while (results.next()) {
                albums.add(results.getString(1));
            }
            return albums;
        } catch (SQLException e) {
            System.out.println("Query Failed | " + e.getMessage());
            return null;
        }
    }


    public List<SongArtist> queryArtistForSong(String songName, int sortOrder) {

        StringBuilder query = new StringBuilder(QUERY_ARTIST_OR_SONG_START);
        query.append(songName);
        query.append("\"");
        query.append(QUERY_ARTIST_OR_SONG_SORT);
        if (sortOrder == ORDER_BY_ASC) {
            query.append(" ASC");
        }
        if (sortOrder == ORDER_BY_DESC) {
            query.append(" DESC");
        }
        System.out.println(query.toString() +
                "\n-----------------------------------------------------------------------");

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(query.toString())) {
            return getSongArtists(results);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void querySongsMetaData() {
        String query = "SELECT * FROM " + TABLE_SONG;
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(query)) {
            ResultSetMetaData metaData = results.getMetaData();
            int numColumns = metaData.getColumnCount();
            for (int i = 1; i < numColumns; i++) {
                System.out.format("columns %d in the songs.table is names %s\n",
                        i, metaData.getColumnName(i));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int getCount(String table) {
        String query = "SELECT COUNT(*) AS count FROM " + table;
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(query)) {
            int count = results.getInt("count");
            return count;
        } catch (SQLException e) {
            System.out.println("Query failed : " + e.getMessage());
            return -1;
        }
    }

    public boolean createViewForSongArtist() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;
        } catch (SQLException e) {
            System.out.println("Create View Failed" + e.getMessage());
            return false;
        }
    }

    public List<SongArtist> querySongInfoView(String songTitle) {
        System.out.println("\n\n=======================================================================\n\n");
        try {
            querySongInfoView.setString(1, songTitle);
            ResultSet results = querySongInfoView.executeQuery();
            return getSongArtists(results);
        } catch (SQLException e) {
            System.out.println("Quer failed " + e.getMessage());
            return null;
        }
    }

    private List<SongArtist> getSongArtists(ResultSet results) throws SQLException {
        List<SongArtist> songArtists = new ArrayList<>();
        while (results.next()) {
            SongArtist songArtist = new SongArtist();
            songArtist.setArtistName(results.getString(1));
            songArtist.setAlbumName(results.getString(2));
            songArtist.setTrack(results.getInt(3));
            songArtists.add(songArtist);
        }
        return songArtists;
    }

    private int insertArtist(String name) throws SQLException {
        queryArtist.setString(1, name);
        ResultSet results = queryArtist.executeQuery();
        if (results.next()) {
            return results.getInt(1);
        } else {
            //insert the artist
            insertIntoArtists.setString(1, name);
            int affectedRows = insertIntoArtists.executeUpdate();
            if (affectedRows != 1) {
                throw new SQLException("Couldn't insert artist");
            }
            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for Artist");
            }
        }
    }

    private int insertAlbum(String name, int artistId) throws SQLException {
        queryAlbum.setString(1, name);
        ResultSet resultSet = queryAlbum.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        } else {
            //insert the albums
            insertIntoAlbums.setString(1, name);
            insertIntoAlbums.setInt(2, artistId);
            int affectedRows = insertIntoAlbums.executeUpdate();
            if (affectedRows != 1) {
                throw new SQLException("Couldn't insert album");
            }
            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for Album");
            }
        }
    }

    public void insertSong(String title, String artist, String album, int track) {

        try {
            querySong.setString(1, title);
            ResultSet resultSet = querySong.executeQuery();
            if (resultSet.next()) {
                return;
            }
            connection.setAutoCommit(false);

            int artistId = insertArtist(artist);
            int albumId = insertAlbum(album, artistId);

            insertIntoSongs.setInt(1, track);
            insertIntoSongs.setString(2, title);

            insertIntoSongs.setInt(3, albumId);

            int affectedRows = insertIntoSongs.executeUpdate();

            if (affectedRows == 1) {
                connection.commit();
            } else {
                throw new SQLException("Song insert Failed");
            }
        } catch (Exception e) {
            System.out.println("Insert Song exception: " + e.getMessage());
            try {
                System.out.println("Performing RollBack");
                connection.rollback();
            } catch (SQLException e2) {
                System.out.println("oh boy! things are really bad!");
            }
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                System.out.println("Couldn't reset autoCommit : " + e.getMessage());
            }
        }

    }
}
