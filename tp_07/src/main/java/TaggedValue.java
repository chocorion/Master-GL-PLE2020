import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

class TaggedValue implements Writable {
    // city format : CountryCode, City,AccentCity,RegionCode,Population,Latitude,Longitude
    // code format : CountryCode, RegionCode, RegionName
    boolean isCity;
    String data;

    String[] splitted;

    public TaggedValue() {}

    private TaggedValue(boolean isCity, String data) {
        this.isCity = isCity;
        this.data = data;

        splitted = this.data.split(",");
    }

    public static Optional<TaggedValue> fromCityData(String data) {
        boolean isHeader = data.split(",")[0].equalsIgnoreCase("country");

        if (isHeader)
            return Optional.empty();

        return Optional.of(new TaggedValue(true, data));
    }

    public static TaggedValue fromCodeData(String data) {
        return new TaggedValue(false, data);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isCity);
        dataOutput.writeUTF(data);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        isCity = dataInput.readBoolean();
        data = dataInput.readUTF();

        splitted = data.split(",");
    }

    public Optional<String> getCountryCode() {
        String countryCode = splitted[0];

        if (countryCode.length() == 0)
            return Optional.empty();

        return Optional.of(countryCode.toLowerCase());
    }

    public Optional<String> getRegionCode() {
        String regionCode = splitted[isCity? 3 : 1];

        if (regionCode.length() == 0)
            return Optional.empty();

        return Optional.of(regionCode.toLowerCase());
    }

    // only if it's region_code data
    public Optional<String> getRegionName() {
        if (isCity || splitted[2].length() == 0)
            return Optional.empty();

        return Optional.of(splitted[2]);
    }

    public Optional<String> getCityName() {
        if (!isCity || splitted[1].length() == 0)
            return Optional.empty();

        return Optional.of(splitted[1]);
    }

    @Override
    public String toString() {
        return "ToggleValue(city : " + isCity + ", data: " + data + ")";
    }
}
